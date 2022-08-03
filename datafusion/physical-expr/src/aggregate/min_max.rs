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
use std::convert::TryFrom;
use std::sync::Arc;

use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::{
    array::{
        ArrayRef, BasicDecimalArray, Date32Array, Date64Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray,
        StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Accumulator, AggregateState};

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::array::Decimal128Array;
use datafusion_row::accessor::RowAccessor;

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
#[derive(Debug)]
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
            &format_state_name(&self.name, "max"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(&self.data_type)?))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        matches!(
            self.data_type,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(MaxRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }
}

// Statically-typed version of min/max(array) -> ScalarValue for string types.
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for non-string types.
macro_rules! typed_min_max_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value)
    }};

    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident, $TZ:expr) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value, $TZ.clone())
    }};
}

// TODO implement this in arrow-rs with simd
// https://github.com/apache/arrow-rs/issues/1010
// Statically-typed version of min/max(array) -> ScalarValue for decimal types.
macro_rules! typed_min_max_batch_decimal128 {
    ($VALUES:expr, $PRECISION:ident, $SCALE:ident, $OP:ident) => {{
        let null_count = $VALUES.null_count();
        if null_count == $VALUES.len() {
            ScalarValue::Decimal128(None, *$PRECISION, *$SCALE)
        } else {
            let array = $VALUES.as_any().downcast_ref::<Decimal128Array>().unwrap();
            if null_count == 0 {
                // there is no null value
                let mut result = array.value(0);
                for i in 1..array.len() {
                    result = result.$OP(array.value(i));
                }
                ScalarValue::Decimal128(Some(result.as_i128()), *$PRECISION, *$SCALE)
            } else {
                let mut result = 0_i128;
                let mut has_value = false;
                for i in 0..array.len() {
                    if !has_value && array.is_valid(i) {
                        has_value = true;
                        result = array.value(i).as_i128();
                    }
                    if array.is_valid(i) {
                        result = result.$OP(array.value(i).as_i128());
                    }
                }
                ScalarValue::Decimal128(Some(result), *$PRECISION, *$SCALE)
            }
        }
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue  for non-string types.
// this is a macro to support both operations (min and max).
macro_rules! min_max_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Decimal(precision, scale) => {
                typed_min_max_batch_decimal128!($VALUES, precision, scale, $OP)
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
            other => {
                // This should have been handled before
                return Err(DataFusionError::Internal(format!(
                    "Min/Max accumulator not implemented for type {:?}",
                    other
                )));
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
        _ => min_max_batch!(values, max),
    })
}
macro_rules! typed_min_max_decimal {
    ($VALUE:expr, $DELTA:expr, $PRECISION:expr, $SCALE:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(
            match ($VALUE, $DELTA) {
                (None, None) => None,
                (Some(a), None) => Some(a.clone()),
                (None, Some(b)) => Some(b.clone()),
                (Some(a), Some(b)) => Some((*a).$OP(*b)),
            },
            $PRECISION.clone(),
            $SCALE.clone(),
        )
    }};
}

// min/max of two non-string scalar values.
macro_rules! typed_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((*a).$OP(*b)),
        })
    }};

    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident, $TZ:expr) => {{
        ScalarValue::$SCALAR(
            match ($VALUE, $DELTA) {
                (None, None) => None,
                (Some(a), None) => Some(a.clone()),
                (None, Some(b)) => Some(b.clone()),
                (Some(a), Some(b)) => Some((*a).$OP(*b)),
            },
            $TZ.clone(),
        )
    }};
}

// min/max of two non-string scalar values.
macro_rules! typed_min_max_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $TYPE:ident, $OP:ident) => {{
        paste::item! {
            match $SCALAR {
                None => {}
                Some(v) => $ACC.[<$OP _ $TYPE>]($INDEX, *v as $TYPE)
            }
        }
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

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        Ok(match ($VALUE, $DELTA) {
            (ScalarValue::Decimal128(lhsv,lhsp,lhss), ScalarValue::Decimal128(rhsv,rhsp,rhss)) => {
                if lhsp.eq(rhsp) && lhss.eq(rhss) {
                    typed_min_max_decimal!(lhsv, rhsv, lhsp, lhss, Decimal128, $OP)
                } else {
                    return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    (ScalarValue::Decimal128(*lhsv,*lhsp,*lhss),ScalarValue::Decimal128(*rhsv,*rhsp,*rhss))
                )));
                }
            }
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Float32(lhs), ScalarValue::Float32(rhs)) => {
                typed_min_max!(lhs, rhs, Float32, $OP)
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
            e => {
                return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )))
            }
        })
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $OP:ident) => {{
        Ok(match $SCALAR {
            ScalarValue::Float64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, f64, $OP)
            }
            ScalarValue::Float32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, f32, $OP)
            }
            ScalarValue::UInt64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u64, $OP)
            }
            ScalarValue::UInt32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u32, $OP)
            }
            ScalarValue::UInt16(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u16, $OP)
            }
            ScalarValue::UInt8(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, u8, $OP)
            }
            ScalarValue::Int64(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i64, $OP)
            }
            ScalarValue::Int32(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i32, $OP)
            }
            ScalarValue::Int16(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i16, $OP)
            }
            ScalarValue::Int8(rhs) => {
                typed_min_max_v2!($INDEX, $ACC, rhs, i8, $OP)
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
                    e
                )))
            }
        })
    }};
}

/// the minimum of two scalar values
pub fn min(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, min)
}

pub fn min_row(index: usize, accessor: &mut RowAccessor, s: &ScalarValue) -> Result<()> {
    min_max_v2!(index, accessor, s, min)
}

/// the maximum of two scalar values
pub fn max(lhs: &ScalarValue, rhs: &ScalarValue) -> Result<ScalarValue> {
    min_max!(lhs, rhs, max)
}

pub fn max_row(index: usize, accessor: &mut RowAccessor, s: &ScalarValue) -> Result<()> {
    min_max_v2!(index, accessor, s, max)
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

    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(self.max.clone())])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.max.clone())
    }
}

#[derive(Debug)]
struct MaxRowAccumulator {
    index: usize,
    data_type: DataType,
}

impl MaxRowAccumulator {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self { index, data_type }
    }
}

impl RowAccumulator for MaxRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &max_batch(values)?;
        max_row(self.index, accessor, delta)?;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.data_type, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

/// MIN aggregate expression
#[derive(Debug)]
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
            &format_state_name(&self.name, "min"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        matches!(
            self.data_type,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
        )
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(MinRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
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
    fn state(&self) -> Result<Vec<AggregateState>> {
        Ok(vec![AggregateState::Scalar(self.min.clone())])
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

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.min.clone())
    }
}

#[derive(Debug)]
struct MinRowAccumulator {
    index: usize,
    data_type: DataType,
}

impl MinRowAccumulator {
    pub fn new(index: usize, data_type: DataType) -> Self {
        Self { index, data_type }
    }
}

impl RowAccumulator for MinRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &min_batch(values)?;
        min_row(self.index, accessor, delta)?;
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.data_type, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_common::ScalarValue::Decimal128;

    #[test]
    fn min_decimal() -> Result<()> {
        // min
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = min(&left, &right)?;
        assert_eq!(result, left);

        // min batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        let result = min_batch(&array)?;
        assert_eq!(result, ScalarValue::Decimal128(Some(1), 10, 0));

        // min batch without values
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(0)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = min_batch(&array)?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // min batch with agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Min,
            ScalarValue::Decimal128(Some(1), 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn min_decimal_all_nulls() -> Result<()> {
        // min batch all nulls
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Min,
            ScalarValue::Decimal128(None, 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn min_decimal_with_nulls() -> Result<()> {
        // min batch with nulls
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Min,
            ScalarValue::Decimal128(Some(1), 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn max_decimal() -> Result<()> {
        // max
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        let result = max(&left, &right)?;
        assert_eq!(result, right);

        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        let result = max(&left, &right);
        let expect = DataFusionError::Internal(format!(
            "MIN/MAX is not expected to receive scalars of incompatible types {:?}",
            (Decimal128(Some(123), 10, 2), Decimal128(Some(124), 10, 3))
        ));
        assert_eq!(expect.to_string(), result.unwrap_err().to_string());

        // max batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 5)?,
        );
        let result = max_batch(&array)?;
        assert_eq!(result, ScalarValue::Decimal128(Some(5), 10, 5));

        // max batch without values
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(0)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        let result = max_batch(&array)?;
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // max batch with agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Max,
            ScalarValue::Decimal128(Some(5), 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn max_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Max,
            ScalarValue::Decimal128(Some(5), 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn max_decimal_all_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            std::iter::repeat(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal(10, 0),
            Min,
            ScalarValue::Decimal128(None, 10, 0),
            DataType::Decimal(10, 0)
        )
    }

    #[test]
    fn max_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::from(5i32),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::from(1i32),
            DataType::Int32
        )
    }

    #[test]
    fn max_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Max,
            ScalarValue::Utf8(Some("d".to_string())),
            DataType::Utf8
        )
    }

    #[test]
    fn max_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Max,
            ScalarValue::LargeUtf8(Some("d".to_string())),
            DataType::LargeUtf8
        )
    }

    #[test]
    fn min_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::Utf8,
            Min,
            ScalarValue::Utf8(Some("a".to_string())),
            DataType::Utf8
        )
    }

    #[test]
    fn min_large_utf8() -> Result<()> {
        let a: ArrayRef = Arc::new(LargeStringArray::from(vec!["d", "a", "c", "b"]));
        generic_test_op!(
            a,
            DataType::LargeUtf8,
            Min,
            ScalarValue::LargeUtf8(Some("a".to_string())),
            DataType::LargeUtf8
        )
    }

    #[test]
    fn max_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::from(5i32),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::from(1i32),
            DataType::Int32
        )
    }

    #[test]
    fn max_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Max,
            ScalarValue::Int32(None),
            DataType::Int32
        )
    }

    #[test]
    fn min_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(
            a,
            DataType::Int32,
            Min,
            ScalarValue::Int32(None),
            DataType::Int32
        )
    }

    #[test]
    fn max_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Max,
            ScalarValue::from(5_u32),
            DataType::UInt32
        )
    }

    #[test]
    fn min_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            Min,
            ScalarValue::from(1u32),
            DataType::UInt32
        )
    }

    #[test]
    fn max_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Max,
            ScalarValue::from(5_f32),
            DataType::Float32
        )
    }

    #[test]
    fn min_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(
            a,
            DataType::Float32,
            Min,
            ScalarValue::from(1_f32),
            DataType::Float32
        )
    }

    #[test]
    fn max_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Max,
            ScalarValue::from(5_f64),
            DataType::Float64
        )
    }

    #[test]
    fn min_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(
            a,
            DataType::Float64,
            Min,
            ScalarValue::from(1_f64),
            DataType::Float64
        )
    }

    #[test]
    fn min_date32() -> Result<()> {
        let a: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Date32,
            Min,
            ScalarValue::Date32(Some(1)),
            DataType::Date32
        )
    }

    #[test]
    fn min_date64() -> Result<()> {
        let a: ArrayRef = Arc::new(Date64Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Date64,
            Min,
            ScalarValue::Date64(Some(1)),
            DataType::Date64
        )
    }

    #[test]
    fn max_date32() -> Result<()> {
        let a: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Date32,
            Max,
            ScalarValue::Date32(Some(5)),
            DataType::Date32
        )
    }

    #[test]
    fn max_date64() -> Result<()> {
        let a: ArrayRef = Arc::new(Date64Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(
            a,
            DataType::Date64,
            Max,
            ScalarValue::Date64(Some(5)),
            DataType::Date64
        )
    }
}
