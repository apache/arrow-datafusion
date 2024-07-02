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
use std::sync::Arc;

use crate::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, IntervalUnit, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::{
    array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
        LargeBinaryArray, LargeStringArray, StringArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use datafusion_common::internal_err;
use datafusion_common::ScalarValue;
use datafusion_common::{downcast_value, DataFusionError, Result};
use datafusion_expr::{Accumulator, GroupsAccumulator};

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::datatypes::i256;
use arrow::datatypes::Decimal256Type;

use super::moving_min_max;

// Min/max aggregation can take Dictionary encode input but always produces unpacked
// (aka non Dictionary) output. We need to adjust the output data type to reflect this.
// The reason min/max aggregate produces unpacked output because there is only one
// min/max value per group; there is no needs to keep them Dictionary encode
fn min_max_aggregate_data_type(input_type: DataType) -> DataType {
    if let DataType::Dictionary(_, value_type) = input_type {
        *value_type
    } else {
        input_type
    }
}

/// MAX aggregate expression
#[derive(Debug, Clone)]
pub struct Max {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Max {
    /// Create a new MAX aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type: min_max_aggregate_data_type(data_type),
            nullable: true,
        }
    }
}
/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MAX`
/// the specified [`ArrowPrimitiveType`].
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_max_accumulator {
    ($SELF:expr, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(
                &$SELF.data_type,
                |cur, new| {
                    if *cur < new {
                        *cur = new
                    }
                },
            )
            // Initialize each accumulator to $NATIVE::MIN
            .with_starting_value($NATIVE::MIN),
        ))
    }};
}

/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MIN`
/// the specified [`ArrowPrimitiveType`].
///
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_min_accumulator {
    ($SELF:expr, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(
                &$SELF.data_type,
                |cur, new| {
                    if *cur > new {
                        *cur = new
                    }
                },
            )
            // Initialize each accumulator to $NATIVE::MAX
            .with_starting_value($NATIVE::MAX),
        ))
    }};
}

impl AggregateExpr for Max {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "max"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![Arc::clone(&self.expr)]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(&self.data_type)?))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;
        matches!(
            self.data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;

        match self.data_type {
            Int8 => instantiate_max_accumulator!(self, i8, Int8Type),
            Int16 => instantiate_max_accumulator!(self, i16, Int16Type),
            Int32 => instantiate_max_accumulator!(self, i32, Int32Type),
            Int64 => instantiate_max_accumulator!(self, i64, Int64Type),
            UInt8 => instantiate_max_accumulator!(self, u8, UInt8Type),
            UInt16 => instantiate_max_accumulator!(self, u16, UInt16Type),
            UInt32 => instantiate_max_accumulator!(self, u32, UInt32Type),
            UInt64 => instantiate_max_accumulator!(self, u64, UInt64Type),
            Float32 => {
                instantiate_max_accumulator!(self, f32, Float32Type)
            }
            Float64 => {
                instantiate_max_accumulator!(self, f64, Float64Type)
            }
            Date32 => instantiate_max_accumulator!(self, i32, Date32Type),
            Date64 => instantiate_max_accumulator!(self, i64, Date64Type),
            Time32(Second) => {
                instantiate_max_accumulator!(self, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                instantiate_max_accumulator!(self, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                instantiate_max_accumulator!(self, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                instantiate_max_accumulator!(self, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                instantiate_max_accumulator!(self, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                instantiate_max_accumulator!(self, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                instantiate_max_accumulator!(self, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_max_accumulator!(self, i64, TimestampNanosecondType)
            }
            Decimal128(_, _) => {
                instantiate_max_accumulator!(self, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                instantiate_max_accumulator!(self, i256, Decimal256Type)
            }

            // It would be nice to have a fast implementation for Strings as well
            // https://github.com/apache/datafusion/issues/6906

            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!(
                "GroupsAccumulator not supported for max({})",
                self.data_type
            ),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMaxAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for Max {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

// Statically-typed version of min/max(array) -> ScalarValue for string types.
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for binay types.
macro_rules! typed_min_max_batch_binary {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_vec()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for non-string types.
macro_rules! typed_min_max_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident $(, $EXTRA_ARGS:ident)*) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value, $($EXTRA_ARGS.clone()),*)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue  for non-string types.
// this is a macro to support both operations (min and max).
macro_rules! min_max_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Decimal128(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal128Array,
                    Decimal128,
                    $OP,
                    precision,
                    scale
                )
            }
            DataType::Decimal256(precision, scale) => {
                typed_min_max_batch!(
                    $VALUES,
                    Decimal256Array,
                    Decimal256,
                    $OP,
                    precision,
                    scale
                )
            }
            // all types that have a natural order
            DataType::Float64 => {
                typed_min_max_batch!($VALUES, Float64Array, Float64, $OP)
            }
            DataType::Float32 => {
                typed_min_max_batch!($VALUES, Float32Array, Float32, $OP)
            }
            DataType::Int64 => typed_min_max_batch!($VALUES, Int64Array, Int64, $OP),
            DataType::Int32 => typed_min_max_batch!($VALUES, Int32Array, Int32, $OP),
            DataType::Int16 => typed_min_max_batch!($VALUES, Int16Array, Int16, $OP),
            DataType::Int8 => typed_min_max_batch!($VALUES, Int8Array, Int8, $OP),
            DataType::UInt64 => typed_min_max_batch!($VALUES, UInt64Array, UInt64, $OP),
            DataType::UInt32 => typed_min_max_batch!($VALUES, UInt32Array, UInt32, $OP),
            DataType::UInt16 => typed_min_max_batch!($VALUES, UInt16Array, UInt16, $OP),
            DataType::UInt8 => typed_min_max_batch!($VALUES, UInt8Array, UInt8, $OP),
            DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                typed_min_max_batch!(
                    $VALUES,
                    TimestampSecondArray,
                    TimestampSecond,
                    $OP,
                    tz_opt
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampMillisecondArray,
                TimestampMillisecond,
                $OP,
                tz_opt
            ),
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                $OP,
                tz_opt
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => typed_min_max_batch!(
                $VALUES,
                TimestampNanosecondArray,
                TimestampNanosecond,
                $OP,
                tz_opt
            ),
            DataType::Date32 => typed_min_max_batch!($VALUES, Date32Array, Date32, $OP),
            DataType::Date64 => typed_min_max_batch!($VALUES, Date64Array, Date64, $OP),
            DataType::Time32(TimeUnit::Second) => {
                typed_min_max_batch!($VALUES, Time32SecondArray, Time32Second, $OP)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time32MillisecondArray,
                    Time32Millisecond,
                    $OP
                )
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time64MicrosecondArray,
                    Time64Microsecond,
                    $OP
                )
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                typed_min_max_batch!(
                    $VALUES,
                    Time64NanosecondArray,
                    Time64Nanosecond,
                    $OP
                )
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                typed_min_max_batch!(
                    $VALUES,
                    IntervalYearMonthArray,
                    IntervalYearMonth,
                    $OP
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                typed_min_max_batch!($VALUES, IntervalDayTimeArray, IntervalDayTime, $OP)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                typed_min_max_batch!(
                    $VALUES,
                    IntervalMonthDayNanoArray,
                    IntervalMonthDayNano,
                    $OP
                )
            }
            other => {
                // This should have been handled before
                return internal_err!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                );
            }
        }
    }};
}

/// dynamically-typed min(array) -> ScalarValue
fn min_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, min_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, min_string)
        }
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, min_boolean)
        }
        DataType::Binary => {
            typed_min_max_batch_binary!(&values, BinaryArray, Binary, min_binary)
        }
        DataType::LargeBinary => {
            typed_min_max_batch_binary!(
                &values,
                LargeBinaryArray,
                LargeBinary,
                min_binary
            )
        }
        _ => min_max_batch!(values, min),
    })
}

/// dynamically-typed max(array) -> ScalarValue
fn max_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, Utf8, max_string)
        }
        DataType::LargeUtf8 => {
            typed_min_max_batch_string!(values, LargeStringArray, LargeUtf8, max_string)
        }
        DataType::Boolean => {
            typed_min_max_batch!(values, BooleanArray, Boolean, max_boolean)
        }
        DataType::Binary => {
            typed_min_max_batch_binary!(&values, BinaryArray, Binary, max_binary)
        }
        DataType::LargeBinary => {
            typed_min_max_batch_binary!(
                &values,
                LargeBinaryArray,
                LargeBinary,
                max_binary
            )
        }
        _ => min_max_batch!(values, max),
    })
}

// min/max of two non-string scalar values.
macro_rules! typed_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident $(, $EXTRA_ARGS:ident)*) => {{
        ScalarValue::$SCALAR(
            match ($VALUE, $DELTA) {
                (None, None) => None,
                (Some(a), None) => Some(*a),
                (None, Some(b)) => Some(*b),
                (Some(a), Some(b)) => Some((*a).$OP(*b)),
            },
            $($EXTRA_ARGS.clone()),*
        )
    }};
}

macro_rules! typed_min_max_float {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(*a),
            (None, Some(b)) => Some(*b),
            (Some(a), Some(b)) => match a.total_cmp(b) {
                choose_min_max!($OP) => Some(*b),
                _ => Some(*a),
            },
        })
    }};
}

// min/max of two scalar string values.
macro_rules! typed_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        })
    }};
}

macro_rules! choose_min_max {
    (min) => {
        std::cmp::Ordering::Greater
    };
    (max) => {
        std::cmp::Ordering::Less
    };
}

macro_rules! interval_min_max {
    ($OP:tt, $LHS:expr, $RHS:expr) => {{
        match $LHS.partial_cmp(&$RHS) {
            Some(choose_min_max!($OP)) => $RHS.clone(),
            Some(_) => $LHS.clone(),
            None => {
                return internal_err!("Comparison error while computing interval min/max")
            }
        }
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        Ok(match ($VALUE, $DELTA) {
            (
                lhs @ ScalarValue::Decimal128(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal128(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal128, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (
                lhs @ ScalarValue::Decimal256(lhsv, lhsp, lhss),
                rhs @ ScalarValue::Decimal256(rhsv, rhsp, rhss)
            ) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max!(lhsv, rhsv, Decimal256, $OP, lhsp, lhss)
                } else {
                    return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (lhs, rhs)
                );
                }
            }
            (ScalarValue::Boolean(lhs), ScalarValue::Boolean(rhs)) => {
                typed_min_max!(lhs, rhs, Boolean, $OP)
            }
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max_float!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
                typed_min_max_float!(lhs, rhs, Float32, $OP)
            }
            (ScalarValue::UInt64(lhs), ScalarValue::UInt64(rhs)) => {
                typed_min_max!(lhs, rhs, UInt64, $OP)
            }
            (ScalarValue::UInt32(lhs), ScalarValue::UInt32(rhs)) => {
                typed_min_max!(lhs, rhs, UInt32, $OP)
            }
            (ScalarValue::UInt16(lhs), ScalarValue::UInt16(rhs)) => {
                typed_min_max!(lhs, rhs, UInt16, $OP)
            }
            (ScalarValue::UInt8(lhs), ScalarValue::UInt8(rhs)) => {
                typed_min_max!(lhs, rhs, UInt8, $OP)
            }
            (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
                typed_min_max!(lhs, rhs, Int64, $OP)
            }
            (ScalarValue::Int32(lhs), ScalarValue::Int32(rhs)) => {
                typed_min_max!(lhs, rhs, Int32, $OP)
            }
            (ScalarValue::Int16(lhs), ScalarValue::Int16(rhs)) => {
                typed_min_max!(lhs, rhs, Int16, $OP)
            }
            (ScalarValue::Int8(lhs), ScalarValue::Int8(rhs)) => {
                typed_min_max!(lhs, rhs, Int8, $OP)
            }
            (ScalarValue::Utf8(lhs), ScalarValue::Utf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, Utf8, $OP)
            }
            (ScalarValue::LargeUtf8(lhs), ScalarValue::LargeUtf8(rhs)) => {
                typed_min_max_string!(lhs, rhs, LargeUtf8, $OP)
            }
            (ScalarValue::Binary(lhs), ScalarValue::Binary(rhs)) => {
                typed_min_max_string!(lhs, rhs, Binary, $OP)
            }
            (ScalarValue::LargeBinary(lhs), ScalarValue::LargeBinary(rhs)) => {
                typed_min_max_string!(lhs, rhs, LargeBinary, $OP)
            }
            (ScalarValue::TimestampSecond(lhs, l_tz), ScalarValue::TimestampSecond(rhs, _)) => {
                typed_min_max!(lhs, rhs, TimestampSecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampMillisecond(lhs, l_tz),
                ScalarValue::TimestampMillisecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampMillisecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampMicrosecond(lhs, l_tz),
                ScalarValue::TimestampMicrosecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampMicrosecond, $OP, l_tz)
            }
            (
                ScalarValue::TimestampNanosecond(lhs, l_tz),
                ScalarValue::TimestampNanosecond(rhs, _),
            ) => {
                typed_min_max!(lhs, rhs, TimestampNanosecond, $OP, l_tz)
            }
            (
                ScalarValue::Date32(lhs),
                ScalarValue::Date32(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Date32, $OP)
            }
            (
                ScalarValue::Date64(lhs),
                ScalarValue::Date64(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Date64, $OP)
            }
            (
                ScalarValue::Time32Second(lhs),
                ScalarValue::Time32Second(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time32Second, $OP)
            }
            (
                ScalarValue::Time32Millisecond(lhs),
                ScalarValue::Time32Millisecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time32Millisecond, $OP)
            }
            (
                ScalarValue::Time64Microsecond(lhs),
                ScalarValue::Time64Microsecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time64Microsecond, $OP)
            }
            (
                ScalarValue::Time64Nanosecond(lhs),
                ScalarValue::Time64Nanosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, Time64Nanosecond, $OP)
            }
            (
                ScalarValue::IntervalYearMonth(lhs),
                ScalarValue::IntervalYearMonth(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalYearMonth, $OP)
            }
            (
                ScalarValue::IntervalMonthDayNano(lhs),
                ScalarValue::IntervalMonthDayNano(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalMonthDayNano, $OP)
            }
            (
                ScalarValue::IntervalDayTime(lhs),
                ScalarValue::IntervalDayTime(rhs),
            ) => {
                typed_min_max!(lhs, rhs, IntervalDayTime, $OP)
            }
            (
                ScalarValue::IntervalYearMonth(_),
                ScalarValue::IntervalMonthDayNano(_),
            ) | (
                ScalarValue::IntervalYearMonth(_),
                ScalarValue::IntervalDayTime(_),
            ) | (
                ScalarValue::IntervalMonthDayNano(_),
                ScalarValue::IntervalDayTime(_),
            ) | (
                ScalarValue::IntervalMonthDayNano(_),
                ScalarValue::IntervalYearMonth(_),
            ) | (
                ScalarValue::IntervalDayTime(_),
                ScalarValue::IntervalYearMonth(_),
            ) | (
                ScalarValue::IntervalDayTime(_),
                ScalarValue::IntervalMonthDayNano(_),
            ) => {
                interval_min_max!($OP, $VALUE, $DELTA)
            }
                    (
                ScalarValue::DurationSecond(lhs),
                ScalarValue::DurationSecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationSecond, $OP)
            }
                                (
                ScalarValue::DurationMillisecond(lhs),
                ScalarValue::DurationMillisecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationMillisecond, $OP)
            }
                                (
                ScalarValue::DurationMicrosecond(lhs),
                ScalarValue::DurationMicrosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationMicrosecond, $OP)
            }
                                        (
                ScalarValue::DurationNanosecond(lhs),
                ScalarValue::DurationNanosecond(rhs),
            ) => {
                typed_min_max!(lhs, rhs, DurationNanosecond, $OP)
            }
            e => {
                return internal_err!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )
            }
        })
    }};
}

/// the minimum of two scalar values
pub fn min(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, min)
}

/// the maximum of two scalar values
pub fn max(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, max)
}

/// An accumulator to compute the maximum value
#[derive(Debug)]
pub struct MaxAccumulator {
    max: ScalarValue,
}

impl MaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &max_batch(values)?;
        self.max = max(&self.max, delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.max) + self.max.size()
    }
}

/// An accumulator to compute the maximum value
#[derive(Debug)]
pub struct SlidingMaxAccumulator {
    max: ScalarValue,
    moving_max: moving_min_max::MovingMax<ScalarValue>,
}

impl SlidingMaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            max: ScalarValue::try_from(datatype)?,
            moving_max: moving_min_max::MovingMax::<ScalarValue>::new(),
        })
    }
}

impl Accumulator for SlidingMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            self.moving_max.push(val);
        }
        if let Some(res) = self.moving_max.max() {
            self.max = res.clone();
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for _idx in 0..values[0].len() {
            (self.moving_max).pop();
        }
        if let Some(res) = self.moving_max.max() {
            self.max = res.clone();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.max.clone()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.max) + self.max.size()
    }
}

/// MIN aggregate expression
#[derive(Debug, Clone)]
pub struct Min {
    name: String,
    data_type: DataType,
    nullable: bool,
    expr: Arc<dyn PhysicalExpr>,
}

impl Min {
    /// Create a new MIN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type: min_max_aggregate_data_type(data_type),
            nullable: true,
        }
    }
}

impl AggregateExpr for Min {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "min"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![Arc::clone(&self.expr)]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;
        matches!(
            self.data_type,
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        match self.data_type {
            Int8 => instantiate_min_accumulator!(self, i8, Int8Type),
            Int16 => instantiate_min_accumulator!(self, i16, Int16Type),
            Int32 => instantiate_min_accumulator!(self, i32, Int32Type),
            Int64 => instantiate_min_accumulator!(self, i64, Int64Type),
            UInt8 => instantiate_min_accumulator!(self, u8, UInt8Type),
            UInt16 => instantiate_min_accumulator!(self, u16, UInt16Type),
            UInt32 => instantiate_min_accumulator!(self, u32, UInt32Type),
            UInt64 => instantiate_min_accumulator!(self, u64, UInt64Type),
            Float32 => {
                instantiate_min_accumulator!(self, f32, Float32Type)
            }
            Float64 => {
                instantiate_min_accumulator!(self, f64, Float64Type)
            }
            Date32 => instantiate_min_accumulator!(self, i32, Date32Type),
            Date64 => instantiate_min_accumulator!(self, i64, Date64Type),
            Time32(Second) => {
                instantiate_min_accumulator!(self, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                instantiate_min_accumulator!(self, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                instantiate_min_accumulator!(self, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                instantiate_min_accumulator!(self, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                instantiate_min_accumulator!(self, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                instantiate_min_accumulator!(self, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                instantiate_min_accumulator!(self, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                instantiate_min_accumulator!(self, i64, TimestampNanosecondType)
            }
            Decimal128(_, _) => {
                instantiate_min_accumulator!(self, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                instantiate_min_accumulator!(self, i256, Decimal256Type)
            }
            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!(
                "GroupsAccumulator not supported for min({})",
                self.data_type
            ),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMinAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for Min {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute the minimum value
#[derive(Debug)]
pub struct MinAccumulator {
    min: ScalarValue,
}

impl MinAccumulator {
    /// new min accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
        })
    }
}

impl Accumulator for MinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.min.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        self.min = min(&self.min, delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

/// An accumulator to compute the minimum value
#[derive(Debug)]
pub struct SlidingMinAccumulator {
    min: ScalarValue,
    moving_min: moving_min_max::MovingMin<ScalarValue>,
}

impl SlidingMinAccumulator {
    /// new min accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            min: ScalarValue::try_from(datatype)?,
            moving_min: moving_min_max::MovingMin::<ScalarValue>::new(),
        })
    }
}

impl Accumulator for SlidingMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.min.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                self.moving_min.push(val);
            }
        }
        if let Some(res) = self.moving_min.min() {
            self.min = res.clone();
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                (self.moving_min).pop();
            }
        }
        if let Some(res) = self.moving_min.min() {
            self.min = res.clone();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.min) + self.min.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{
        IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    };

    #[test]
    fn interval_min_max() {
        // IntervalYearMonth
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(0, 1),
            IntervalYearMonthType::make_value(5, 34),
            IntervalYearMonthType::make_value(-2, 4),
            IntervalYearMonthType::make_value(7, -4),
            IntervalYearMonthType::make_value(0, 1),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                -2, 4
            )))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                5, 34
            )))
        );

        // IntervalDayTime
        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(0, 0),
            IntervalDayTimeType::make_value(5, 454000),
            IntervalDayTimeType::make_value(-34, 0),
            IntervalDayTimeType::make_value(7, -4000),
            IntervalDayTimeType::make_value(1, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(-34, 0)))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(7, -4000)))
        );

        // IntervalMonthDayNano
        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(1, 0, 0),
            IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
            IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
            IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
            IntervalMonthDayNanoType::make_value(1, 0, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000)
            ))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000)
            ))
        );
    }

    #[test]
    fn float_min_max_with_nans() {
        let pos_nan = f32::NAN;
        let zero = 0_f32;
        let neg_inf = f32::NEG_INFINITY;

        let check = |acc: &mut dyn Accumulator, values: &[&[f32]], expected: f32| {
            for batch in values.iter() {
                let batch =
                    Arc::new(Float32Array::from_iter_values(batch.iter().copied()));
                acc.update_batch(&[batch]).unwrap();
            }
            let result = acc.evaluate().unwrap();
            assert_eq!(result, ScalarValue::Float32(Some(expected)));
        };

        // This test checks both comparison between batches (which uses the min_max macro
        // defined above) and within a batch (which uses the arrow min/max compute function
        // and verifies both respect the total order comparison for floats)

        let min = || MinAccumulator::try_new(&DataType::Float32).unwrap();
        let max = || MaxAccumulator::try_new(&DataType::Float32).unwrap();

        check(&mut min(), &[&[zero], &[pos_nan]], zero);
        check(&mut min(), &[&[zero, pos_nan]], zero);
        check(&mut min(), &[&[zero], &[neg_inf]], neg_inf);
        check(&mut min(), &[&[zero, neg_inf]], neg_inf);
        check(&mut max(), &[&[zero], &[pos_nan]], pos_nan);
        check(&mut max(), &[&[zero, pos_nan]], pos_nan);
        check(&mut max(), &[&[zero], &[neg_inf]], zero);
        check(&mut max(), &[&[zero, neg_inf]], zero);
    }
}
