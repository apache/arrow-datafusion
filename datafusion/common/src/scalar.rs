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

//! This module provides ScalarValue, an enum that can be used for storage of single elements

use crate::error::{DataFusionError, Result};
use arrow::{
    array::*,
    compute::kernels::cast::{cast, cast_with_options, CastOptions},
    datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, DataType, Field, Float32Type,
        Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
        IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        DECIMAL128_MAX_PRECISION,
    },
    util::decimal::{BasicDecimal, Decimal128},
};
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::convert::{Infallible, TryInto};
use std::str::FromStr;
use std::{convert::TryFrom, fmt, iter::repeat, sync::Arc};

/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part of arrow’s `Array`.
/// https://arrow.apache.org/docs/python/api/datatypes.html
/// https://github.com/apache/arrow/blob/master/format/Schema.fbs#L354-L375
#[derive(Clone)]
pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(Option<bool>),
    /// 32bit float
    Float32(Option<f32>),
    /// 64bit float
    Float64(Option<f64>),
    /// 128bit decimal, using the i128 to represent the decimal
    Decimal128(Option<i128>, usize, usize),
    /// signed 8bit int
    Int8(Option<i8>),
    /// signed 16bit int
    Int16(Option<i16>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// unsigned 8bit int
    UInt8(Option<u8>),
    /// unsigned 16bit int
    UInt16(Option<u16>),
    /// unsigned 32bit int
    UInt32(Option<u32>),
    /// unsigned 64bit int
    UInt64(Option<u64>),
    /// utf-8 encoded string.
    Utf8(Option<String>),
    /// utf-8 encoded string representing a LargeString's arrow type.
    LargeUtf8(Option<String>),
    /// binary
    Binary(Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// list of nested ScalarValue
    List(Option<Vec<ScalarValue>>, Box<Field>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<String>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<String>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<String>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<String>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<i64>),
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// Months and days are encoded as 32-bit signed integers.
    /// Nanoseconds is encoded as a 64-bit signed integer (no leap seconds).
    IntervalMonthDayNano(Option<i128>),
    /// struct of nested ScalarValue
    Struct(Option<Vec<ScalarValue>>, Box<Vec<Field>>),
    /// Dictionary type: index type and value
    Dictionary(Box<DataType>, Box<ScalarValue>),
}

// manual implementation of `PartialEq` that uses OrderedFloat to
// get defined behavior for floating point
impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                v1.eq(v2) && p1.eq(p2) && s1.eq(s2)
            }
            (Decimal128(_, _, _), _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1.eq(v2),
            (Utf8(_), _) => false,
            (LargeUtf8(v1), LargeUtf8(v2)) => v1.eq(v2),
            (LargeUtf8(_), _) => false,
            (Binary(v1), Binary(v2)) => v1.eq(v2),
            (Binary(_), _) => false,
            (LargeBinary(v1), LargeBinary(v2)) => v1.eq(v2),
            (LargeBinary(_), _) => false,
            (List(v1, t1), List(v2, t2)) => v1.eq(v2) && t1.eq(t2),
            (List(_, _), _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.eq(v2),
            (TimestampSecond(_, _), _) => false,
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => v1.eq(v2),
            (TimestampMillisecond(_, _), _) => false,
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => v1.eq(v2),
            (TimestampMicrosecond(_, _), _) => false,
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => v1.eq(v2),
            (TimestampNanosecond(_, _), _) => false,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.eq(v2),
            (IntervalYearMonth(_), _) => false,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.eq(v2),
            (IntervalDayTime(_), _) => false,
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1.eq(v2),
            (IntervalMonthDayNano(_), _) => false,
            (Struct(v1, t1), Struct(v2, t2)) => v1.eq(v2) && t1.eq(t2),
            (Struct(_, _), _) => false,
            (Dictionary(k1, v1), Dictionary(k2, v2)) => k1.eq(k2) && v1.eq(v2),
            (Dictionary(_, _), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
        }
    }
}

// manual implementation of `PartialOrd` that uses OrderedFloat to
// get defined behavior for floating point
impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScalarValue::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (self, other) {
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.partial_cmp(v2)
                } else {
                    // Two decimal values can be compared if they have the same precision and scale.
                    None
                }
            }
            (Decimal128(_, _, _), _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
            (Utf8(_), _) => None,
            (LargeUtf8(v1), LargeUtf8(v2)) => v1.partial_cmp(v2),
            (LargeUtf8(_), _) => None,
            (Binary(v1), Binary(v2)) => v1.partial_cmp(v2),
            (Binary(_), _) => None,
            (LargeBinary(v1), LargeBinary(v2)) => v1.partial_cmp(v2),
            (LargeBinary(_), _) => None,
            (List(v1, t1), List(v2, t2)) => {
                if t1.eq(t2) {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (List(_, _), _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.partial_cmp(v2),
            (TimestampSecond(_, _), _) => None,
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampMillisecond(_, _), _) => None,
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampMicrosecond(_, _), _) => None,
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => {
                v1.partial_cmp(v2)
            }
            (TimestampNanosecond(_, _), _) => None,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.partial_cmp(v2),
            (IntervalYearMonth(_), _) => None,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.partial_cmp(v2),
            (IntervalDayTime(_), _) => None,
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1.partial_cmp(v2),
            (IntervalMonthDayNano(_), _) => None,
            (Struct(v1, t1), Struct(v2, t2)) => {
                if t1.eq(t2) {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Struct(_, _), _) => None,
            (Dictionary(k1, v1), Dictionary(k2, v2)) => {
                // Don't compare if the key types don't match (it is effectively a different datatype)
                if k1 == k2 {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Dictionary(_, _), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
        }
    }
}

impl Eq for ScalarValue {}

// manual implementation of `Hash` that uses OrderedFloat to
// get defined behavior for floating point
impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state)
            }
            Boolean(v) => v.hash(state),
            Float32(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Float64(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            LargeUtf8(v) => v.hash(state),
            Binary(v) => v.hash(state),
            LargeBinary(v) => v.hash(state),
            List(v, t) => {
                v.hash(state);
                t.hash(state);
            }
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            TimestampSecond(v, _) => v.hash(state),
            TimestampMillisecond(v, _) => v.hash(state),
            TimestampMicrosecond(v, _) => v.hash(state),
            TimestampNanosecond(v, _) => v.hash(state),
            IntervalYearMonth(v) => v.hash(state),
            IntervalDayTime(v) => v.hash(state),
            IntervalMonthDayNano(v) => v.hash(state),
            Struct(v, t) => {
                v.hash(state);
                t.hash(state);
            }
            Dictionary(k, v) => {
                k.hash(state);
                v.hash(state);
            }
            // stable hash for Null value
            Null => 1.hash(state),
        }
    }
}

/// return a reference to the values array and the index into it for a
/// dictionary array
#[inline]
fn get_dict_value<K: ArrowDictionaryKeyType>(
    array: &ArrayRef,
    index: usize,
) -> (&ArrayRef, Option<usize>) {
    let dict_array = as_dictionary_array::<K>(array);
    (dict_array.values(), dict_array.key(index))
}

/// Create a dictionary array representing `value` repeated `size`
/// times
fn dict_from_scalar<K: ArrowDictionaryKeyType>(
    value: &ScalarValue,
    size: usize,
) -> ArrayRef {
    // values array is one element long (the value)
    let values_array = value.to_array_of_size(1);

    // Create a key array with `size` elements, each of 0
    let key_array: PrimitiveArray<K> = std::iter::repeat(Some(K::default_value()))
        .take(size)
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    Arc::new(
        DictionaryArray::<K>::try_new(&key_array, &values_array)
            // should always be valid by construction above
            .expect("Can not construct dictionary array"),
    )
}

/// Create a dictionary array representing all the values in values
fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: &dyn Array,
) -> Result<ArrayRef> {
    // Create a key array with `size` elements of 0..array_len for all
    // non-null value elements
    let key_array: PrimitiveArray<K> = (0..values_array.len())
        .map(|index| {
            if values_array.is_valid(index) {
                let native_index = K::Native::from_usize(index).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Can not create index of type {} from value {}",
                        K::DATA_TYPE,
                        index
                    ))
                })?;
                Ok(Some(native_index))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    let dict_array = DictionaryArray::<K>::try_new(&key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

macro_rules! typed_cast_tz {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident, $TZ:expr) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(
            match array.is_null($index) {
                true => None,
                false => Some(array.value($index).into()),
            },
            $TZ.clone(),
        )
    }};
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! build_list {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        match $VALUES {
            // the return on the macro is necessary, to short-circuit and return ArrayRef
            None => {
                return new_null_array(
                    &DataType::List(Box::new(Field::new(
                        "item",
                        DataType::$SCALAR_TY,
                        true,
                    ))),
                    $SIZE,
                )
            }
            Some(values) => {
                build_values_list!($VALUE_BUILDER_TY, $SCALAR_TY, values, $SIZE)
            }
        }
    }};
}

macro_rules! build_timestamp_list {
    ($TIME_UNIT:expr, $TIME_ZONE:expr, $VALUES:expr, $SIZE:expr) => {{
        match $VALUES {
            // the return on the macro is necessary, to short-circuit and return ArrayRef
            None => {
                return new_null_array(
                    &DataType::List(Box::new(Field::new(
                        "item",
                        DataType::Timestamp($TIME_UNIT, $TIME_ZONE),
                        true,
                    ))),
                    $SIZE,
                )
            }
            Some(values) => match $TIME_UNIT {
                TimeUnit::Second => {
                    build_values_list_tz!(
                        TimestampSecondBuilder,
                        TimestampSecond,
                        values,
                        $SIZE
                    )
                }
                TimeUnit::Microsecond => build_values_list_tz!(
                    TimestampMillisecondBuilder,
                    TimestampMillisecond,
                    values,
                    $SIZE
                ),
                TimeUnit::Millisecond => build_values_list_tz!(
                    TimestampMicrosecondBuilder,
                    TimestampMicrosecond,
                    values,
                    $SIZE
                ),
                TimeUnit::Nanosecond => build_values_list_tz!(
                    TimestampNanosecondBuilder,
                    TimestampNanosecond,
                    values,
                    $SIZE
                ),
            },
        }
    }};
}

macro_rules! build_values_list {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new($VALUES.len()));

        for _ in 0..$SIZE {
            for scalar_value in $VALUES {
                match scalar_value {
                    ScalarValue::$SCALAR_TY(Some(v)) => {
                        builder.values().append_value(v.clone());
                    }
                    ScalarValue::$SCALAR_TY(None) => {
                        builder.values().append_null();
                    }
                    _ => panic!("Incompatible ScalarValue for list"),
                };
            }
            builder.append(true);
        }

        builder.finish()
    }};
}

macro_rules! build_values_list_tz {
    ($VALUE_BUILDER_TY:ident, $SCALAR_TY:ident, $VALUES:expr, $SIZE:expr) => {{
        let mut builder = ListBuilder::new($VALUE_BUILDER_TY::new($VALUES.len()));

        for _ in 0..$SIZE {
            for scalar_value in $VALUES {
                match scalar_value {
                    ScalarValue::$SCALAR_TY(Some(v), _) => {
                        builder.values().append_value(v.clone());
                    }
                    ScalarValue::$SCALAR_TY(None, _) => {
                        builder.values().append_null();
                    }
                    _ => panic!("Incompatible ScalarValue for list"),
                };
            }
            builder.append(true);
        }

        builder.finish()
    }};
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE($ENUM), $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ENUM2:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => {
                let array: ArrayRef = Arc::new($ARRAY_TYPE::from_value(*value, $SIZE));
                // Need to call cast to cast to final data type with timezone/extra param
                cast(&array, &DataType::$DATA_TYPE($ENUM, $ENUM2))
                    .expect("cannot do temporal cast")
            }
            None => new_null_array(&DataType::$DATA_TYPE($ENUM, $ENUM2), $SIZE),
        }
    }};
}

macro_rules! eq_array_primitive {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $VALUE:expr) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let is_valid = array.is_valid($index);
        match $VALUE {
            Some(val) => is_valid && &array.value($index) == val,
            None => !is_valid,
        }
    }};
}

impl ScalarValue {
    /// Create a decimal Scalar from value/precision and scale.
    pub fn try_new_decimal128(
        value: i128,
        precision: usize,
        scale: usize,
    ) -> Result<Self> {
        // make sure the precision and scale is valid
        if precision <= DECIMAL128_MAX_PRECISION && scale <= precision {
            return Ok(ScalarValue::Decimal128(Some(value), precision, scale));
        }
        Err(DataFusionError::Internal(format!(
            "Can not new a decimal type ScalarValue for precision {} and scale {}",
            precision, scale
        )))
    }

    /// Returns a [`ScalarValue::IntervalYearMonth`] representing
    /// `years` years and `months` months
    pub fn new_interval_ym(years: i32, months: i32) -> Self {
        let val = IntervalYearMonthType::make_value(years, months);
        ScalarValue::IntervalYearMonth(Some(val))
    }

    /// Returns a [`ScalarValue::IntervalDayTime`] representing
    /// `days` days and `millis` milliseconds
    pub fn new_interval_dt(days: i32, millis: i32) -> Self {
        let val = IntervalDayTimeType::make_value(days, millis);
        Self::IntervalDayTime(Some(val))
    }

    /// Returns a [`ScalarValue::IntervalMonthDayNano`] representing
    /// `months` months and `days` days, and `nanos` nanoseconds
    pub fn new_interval_mdn(months: i32, days: i32, nanos: i64) -> Self {
        let val = IntervalMonthDayNanoType::make_value(months, days, nanos);
        ScalarValue::IntervalMonthDayNano(Some(val))
    }

    /// Getter for the `DataType` of the value
    pub fn get_datatype(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Decimal128(_, precision, scale) => {
                DataType::Decimal128(*precision, *scale)
            }
            ScalarValue::TimestampSecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Second, tz_opt.clone())
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Millisecond, tz_opt.clone())
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Microsecond, tz_opt.clone())
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
            }
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Binary(_) => DataType::Binary,
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::List(_, field) => DataType::List(Box::new(Field::new(
                "item",
                field.data_type().clone(),
                true,
            ))),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::IntervalYearMonth(_) => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::IntervalMonthDayNano(_) => {
                DataType::Interval(IntervalUnit::MonthDayNano)
            }
            ScalarValue::Struct(_, fields) => DataType::Struct(fields.as_ref().clone()),
            ScalarValue::Dictionary(k, v) => {
                DataType::Dictionary(k.clone(), Box::new(v.get_datatype()))
            }
            ScalarValue::Null => DataType::Null,
        }
    }

    /// Calculate arithmetic negation for a scalar value
    pub fn arithmetic_negate(&self) -> Self {
        match self {
            ScalarValue::Boolean(None)
            | ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Float32(None) => self.clone(),
            ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(-v)),
            ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(-v)),
            ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(-v)),
            ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(-v)),
            ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(-v)),
            ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(-v)),
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                ScalarValue::Decimal128(Some(-v), *precision, *scale)
            }
            _ => panic!("Cannot run arithmetic negate on scalar value: {:?}", self),
        }
    }

    /// whether this value is null or not.
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Null => true,
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
            ScalarValue::Int8(v) => v.is_none(),
            ScalarValue::Int16(v) => v.is_none(),
            ScalarValue::Int32(v) => v.is_none(),
            ScalarValue::Int64(v) => v.is_none(),
            ScalarValue::UInt8(v) => v.is_none(),
            ScalarValue::UInt16(v) => v.is_none(),
            ScalarValue::UInt32(v) => v.is_none(),
            ScalarValue::UInt64(v) => v.is_none(),
            ScalarValue::Utf8(v) => v.is_none(),
            ScalarValue::LargeUtf8(v) => v.is_none(),
            ScalarValue::Binary(v) => v.is_none(),
            ScalarValue::LargeBinary(v) => v.is_none(),
            ScalarValue::List(v, _) => v.is_none(),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::TimestampSecond(v, _) => v.is_none(),
            ScalarValue::TimestampMillisecond(v, _) => v.is_none(),
            ScalarValue::TimestampMicrosecond(v, _) => v.is_none(),
            ScalarValue::TimestampNanosecond(v, _) => v.is_none(),
            ScalarValue::IntervalYearMonth(v) => v.is_none(),
            ScalarValue::IntervalDayTime(v) => v.is_none(),
            ScalarValue::IntervalMonthDayNano(v) => v.is_none(),
            ScalarValue::Struct(v, _) => v.is_none(),
            ScalarValue::Dictionary(_, v) => v.is_null(),
        }
    }

    /// Converts a scalar value into an 1-row array.
    pub fn to_array(&self) -> ArrayRef {
        self.to_array_of_size(1)
    }

    /// Converts an iterator of references [`ScalarValue`] into an [`ArrayRef`]
    /// corresponding to those values. For example,
    ///
    /// Returns an error if the iterator is empty or if the
    /// [`ScalarValue`]s are not all the same type
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{ArrayRef, BooleanArray};
    ///
    /// let scalars = vec![
    ///   ScalarValue::Boolean(Some(true)),
    ///   ScalarValue::Boolean(None),
    ///   ScalarValue::Boolean(Some(false)),
    /// ];
    ///
    /// // Build an Array from the list of ScalarValues
    /// let array = ScalarValue::iter_to_array(scalars.into_iter())
    ///   .unwrap();
    ///
    /// let expected: ArrayRef = std::sync::Arc::new(
    ///   BooleanArray::from(vec![
    ///     Some(true),
    ///     None,
    ///     Some(false)
    ///   ]
    /// ));
    ///
    /// assert_eq!(&array, &expected);
    /// ```
    pub fn iter_to_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<ArrayRef> {
        let mut scalars = scalars.into_iter().peekable();

        // figure out the type based on the first element
        let data_type = match scalars.peek() {
            None => {
                return Err(DataFusionError::Internal(
                    "Empty iterator passed to ScalarValue::iter_to_array".to_string(),
                ));
            }
            Some(sv) => sv.get_datatype(),
        };

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for primitive types
        macro_rules! build_array_primitive {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        macro_rules! build_array_primitive_tz {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v, _) = sv {
                            Ok(v)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        /// Creates an array of $ARRAY_TY by unpacking values of
        /// SCALAR_TY for "string-like" types.
        macro_rules! build_array_string {
            ($ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v) = sv {
                            Ok(v)
                        } else {
                            Err(DataFusionError::Internal(format!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )))
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        macro_rules! build_array_list_primitive {
            ($ARRAY_TY:ident, $SCALAR_TY:ident, $NATIVE_TYPE:ident) => {{
                Arc::new(ListArray::from_iter_primitive::<$ARRAY_TY, _, _>(
                    scalars.into_iter().map(|x| match x {
                        ScalarValue::List(xs, _) => xs.map(|x| {
                            x.iter().map(|x| match x {
                                ScalarValue::$SCALAR_TY(i) => *i,
                                sv => panic!(
                                    "Inconsistent types in ScalarValue::iter_to_array. \
                                        Expected {:?}, got {:?}",
                                    data_type, sv
                                ),
                            })
                            .collect::<Vec<Option<$NATIVE_TYPE>>>()
                        }),
                        sv => panic!(
                            "Inconsistent types in ScalarValue::iter_to_array. \
                                Expected {:?}, got {:?}",
                            data_type, sv
                        ),
                    }),
                ))
            }};
        }

        macro_rules! build_array_list_string {
            ($BUILDER:ident, $SCALAR_TY:ident) => {{
                let mut builder = ListBuilder::new($BUILDER::new(0));
                for scalar in scalars.into_iter() {
                    match scalar {
                        ScalarValue::List(Some(xs), _) => {
                            for s in xs {
                                match s {
                                    ScalarValue::$SCALAR_TY(Some(val)) => {
                                        builder.values().append_value(val);
                                    }
                                    ScalarValue::$SCALAR_TY(None) => {
                                        builder.values().append_null();
                                    }
                                    sv => {
                                        return Err(DataFusionError::Internal(format!(
                                            "Inconsistent types in ScalarValue::iter_to_array. \
                                                Expected Utf8, got {:?}",
                                            sv
                                        )))
                                    }
                                }
                            }
                            builder.append(true);
                        }
                        ScalarValue::List(None, _) => {
                            builder.append(false);
                        }
                        sv => {
                            return Err(DataFusionError::Internal(format!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected List, got {:?}",
                                sv
                            )))
                        }
                    }
                }
                Arc::new(builder.finish())
            }};
        }

        let array: ArrayRef = match &data_type {
            DataType::Decimal128(precision, scale) => {
                let decimal_array =
                    ScalarValue::iter_to_decimal_array(scalars, precision, scale)?;
                Arc::new(decimal_array)
            }
            DataType::Decimal256(_, _) => {
                return Err(DataFusionError::Internal(
                    "Decimal256 is not supported for ScalarValue".to_string(),
                ))
            }
            DataType::Null => ScalarValue::iter_to_null_array(scalars),
            DataType::Boolean => build_array_primitive!(BooleanArray, Boolean),
            DataType::Float32 => build_array_primitive!(Float32Array, Float32),
            DataType::Float64 => build_array_primitive!(Float64Array, Float64),
            DataType::Int8 => build_array_primitive!(Int8Array, Int8),
            DataType::Int16 => build_array_primitive!(Int16Array, Int16),
            DataType::Int32 => build_array_primitive!(Int32Array, Int32),
            DataType::Int64 => build_array_primitive!(Int64Array, Int64),
            DataType::UInt8 => build_array_primitive!(UInt8Array, UInt8),
            DataType::UInt16 => build_array_primitive!(UInt16Array, UInt16),
            DataType::UInt32 => build_array_primitive!(UInt32Array, UInt32),
            DataType::UInt64 => build_array_primitive!(UInt64Array, UInt64),
            DataType::Utf8 => build_array_string!(StringArray, Utf8),
            DataType::LargeUtf8 => build_array_string!(LargeStringArray, LargeUtf8),
            DataType::Binary => build_array_string!(BinaryArray, Binary),
            DataType::LargeBinary => build_array_string!(LargeBinaryArray, LargeBinary),
            DataType::Date32 => build_array_primitive!(Date32Array, Date32),
            DataType::Date64 => build_array_primitive!(Date64Array, Date64),
            DataType::Timestamp(TimeUnit::Second, _) => {
                build_array_primitive_tz!(TimestampSecondArray, TimestampSecond)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                build_array_primitive_tz!(TimestampMillisecondArray, TimestampMillisecond)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                build_array_primitive_tz!(TimestampMicrosecondArray, TimestampMicrosecond)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                build_array_primitive_tz!(TimestampNanosecondArray, TimestampNanosecond)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                build_array_primitive!(IntervalDayTimeArray, IntervalDayTime)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                build_array_primitive!(IntervalYearMonthArray, IntervalYearMonth)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int8 => {
                build_array_list_primitive!(Int8Type, Int8, i8)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int16 => {
                build_array_list_primitive!(Int16Type, Int16, i16)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int32 => {
                build_array_list_primitive!(Int32Type, Int32, i32)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int64 => {
                build_array_list_primitive!(Int64Type, Int64, i64)
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt8 => {
                build_array_list_primitive!(UInt8Type, UInt8, u8)
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt16 => {
                build_array_list_primitive!(UInt16Type, UInt16, u16)
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt32 => {
                build_array_list_primitive!(UInt32Type, UInt32, u32)
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt64 => {
                build_array_list_primitive!(UInt64Type, UInt64, u64)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Float32 => {
                build_array_list_primitive!(Float32Type, Float32, f32)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Float64 => {
                build_array_list_primitive!(Float64Type, Float64, f64)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Utf8 => {
                build_array_list_string!(StringBuilder, Utf8)
            }
            DataType::List(fields) if fields.data_type() == &DataType::LargeUtf8 => {
                build_array_list_string!(LargeStringBuilder, LargeUtf8)
            }
            DataType::List(_) => {
                // Fallback case handling homogeneous lists with any ScalarValue element type
                let list_array = ScalarValue::iter_to_array_list(scalars, &data_type)?;
                Arc::new(list_array)
            }
            DataType::Struct(fields) => {
                // Initialize a Vector to store the ScalarValues for each column
                let mut columns: Vec<Vec<ScalarValue>> =
                    (0..fields.len()).map(|_| Vec::new()).collect();

                // Iterate over scalars to populate the column scalars for each row
                for scalar in scalars {
                    if let ScalarValue::Struct(values, fields) = scalar {
                        match values {
                            Some(values) => {
                                // Push value for each field
                                for (column, value) in columns.iter_mut().zip(values) {
                                    column.push(value.clone());
                                }
                            }
                            None => {
                                // Push NULL of the appropriate type for each field
                                for (column, field) in
                                    columns.iter_mut().zip(fields.as_ref())
                                {
                                    column
                                        .push(ScalarValue::try_from(field.data_type())?);
                                }
                            }
                        };
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "Expected Struct but found: {}",
                            scalar
                        )));
                    };
                }

                // Call iter_to_array recursively to convert the scalars for each column into Arrow arrays
                let field_values = fields
                    .iter()
                    .zip(columns)
                    .map(|(field, column)| -> Result<(Field, ArrayRef)> {
                        Ok((field.clone(), Self::iter_to_array(column)?))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Arc::new(StructArray::from(field_values))
            }
            DataType::Dictionary(key_type, value_type) => {
                // create the values array
                let value_scalars = scalars
                    .into_iter()
                    .map(|scalar| match scalar {
                        ScalarValue::Dictionary(inner_key_type, scalar) => {
                            if &inner_key_type == key_type {
                                Ok(*scalar)
                            } else{
                                panic!("Expected inner key type of {} but found: {}, value was ({:?})", key_type, inner_key_type, scalar);
                            }
                        },
                        _ => {
                            Err(DataFusionError::Internal(format!(
                                "Expected scalar of type {} but found: {} {:?}",
                                value_type, scalar, scalar
                            )))
                        },
                    })
                    .collect::<Result<Vec<_>>>()?;

                let values = Self::iter_to_array(value_scalars)?;
                assert_eq!(values.data_type(), value_type.as_ref());

                match key_type.as_ref() {
                    DataType::Int8 => dict_from_values::<Int8Type>(&values)?,
                    DataType::Int16 => dict_from_values::<Int16Type>(&values)?,
                    DataType::Int32 => dict_from_values::<Int32Type>(&values)?,
                    DataType::Int64 => dict_from_values::<Int64Type>(&values)?,
                    DataType::UInt8 => dict_from_values::<UInt8Type>(&values)?,
                    DataType::UInt16 => dict_from_values::<UInt16Type>(&values)?,
                    DataType::UInt32 => dict_from_values::<UInt32Type>(&values)?,
                    DataType::UInt64 => dict_from_values::<UInt64Type>(&values)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            // explicitly enumerate unsupported types so newly added
            // types must be aknowledged
            DataType::Float16
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::FixedSizeBinary(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::Union(_, _, _)
            | DataType::Map(_, _) => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported creation of {:?} array from ScalarValue {:?}",
                    data_type,
                    scalars.peek()
                )));
            }
        };

        Ok(array)
    }

    fn iter_to_null_array(scalars: impl IntoIterator<Item = ScalarValue>) -> ArrayRef {
        let length =
            scalars
                .into_iter()
                .fold(0usize, |r, element: ScalarValue| match element {
                    ScalarValue::Null => r + 1,
                    _ => unreachable!(),
                });
        new_null_array(&DataType::Null, length)
    }

    fn iter_to_decimal_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
        precision: &usize,
        scale: &usize,
    ) -> Result<Decimal128Array> {
        let array = scalars
            .into_iter()
            .map(|element: ScalarValue| match element {
                ScalarValue::Decimal128(v1, _, _) => v1,
                _ => unreachable!(),
            })
            .collect::<Decimal128Array>()
            .with_precision_and_scale(*precision, *scale)?;
        Ok(array)
    }

    fn iter_to_array_list(
        scalars: impl IntoIterator<Item = ScalarValue>,
        data_type: &DataType,
    ) -> Result<GenericListArray<i32>> {
        let mut offsets = Int32Array::builder(0);
        offsets.append_value(0);

        let mut elements: Vec<ArrayRef> = Vec::new();
        let mut valid = BooleanBufferBuilder::new(0);
        let mut flat_len = 0i32;
        for scalar in scalars {
            if let ScalarValue::List(values, _) = scalar {
                match values {
                    Some(values) => {
                        let element_array = ScalarValue::iter_to_array(values)?;

                        // Add new offset index
                        flat_len += element_array.len() as i32;
                        offsets.append_value(flat_len);

                        elements.push(element_array);

                        // Element is valid
                        valid.append(true);
                    }
                    None => {
                        // Repeat previous offset index
                        offsets.append_value(flat_len);

                        // Element is null
                        valid.append(false);
                    }
                }
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Expected ScalarValue::List element. Received {:?}",
                    scalar
                )));
            }
        }

        // Concatenate element arrays to create single flat array
        let element_arrays: Vec<&dyn Array> =
            elements.iter().map(|a| a.as_ref()).collect();
        let flat_array = match arrow::compute::concat(&element_arrays) {
            Ok(flat_array) => flat_array,
            Err(err) => return Err(DataFusionError::ArrowError(err)),
        };

        // Build ListArray using ArrayData so we can specify a flat inner array, and offset indices
        let offsets_array = offsets.finish();
        let array_data = ArrayDataBuilder::new(data_type.clone())
            .len(offsets_array.len() - 1)
            .null_bit_buffer(Some(valid.finish()))
            .add_buffer(offsets_array.data().buffers()[0].clone())
            .add_child_data(flat_array.data().clone());

        let list_array = ListArray::from(array_data.build()?);
        Ok(list_array)
    }

    fn build_decimal_array(
        value: &Option<i128>,
        precision: &usize,
        scale: &usize,
        size: usize,
    ) -> Decimal128Array {
        std::iter::repeat(value)
            .take(size)
            .collect::<Decimal128Array>()
            .with_precision_and_scale(*precision, *scale)
            .unwrap()
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Decimal128(e, precision, scale) => {
                Arc::new(ScalarValue::build_decimal_array(e, precision, scale, size))
            }
            ScalarValue::Boolean(e) => {
                Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef
            }
            ScalarValue::Float64(e) => {
                build_array_from_option!(Float64, Float64Array, e, size)
            }
            ScalarValue::Float32(e) => {
                build_array_from_option!(Float32, Float32Array, e, size)
            }
            ScalarValue::Int8(e) => build_array_from_option!(Int8, Int8Array, e, size),
            ScalarValue::Int16(e) => build_array_from_option!(Int16, Int16Array, e, size),
            ScalarValue::Int32(e) => build_array_from_option!(Int32, Int32Array, e, size),
            ScalarValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            ScalarValue::UInt8(e) => build_array_from_option!(UInt8, UInt8Array, e, size),
            ScalarValue::UInt16(e) => {
                build_array_from_option!(UInt16, UInt16Array, e, size)
            }
            ScalarValue::UInt32(e) => {
                build_array_from_option!(UInt32, UInt32Array, e, size)
            }
            ScalarValue::UInt64(e) => {
                build_array_from_option!(UInt64, UInt64Array, e, size)
            }
            ScalarValue::TimestampSecond(e, tz_opt) => build_array_from_option!(
                Timestamp,
                TimeUnit::Second,
                tz_opt.clone(),
                TimestampSecondArray,
                e,
                size
            ),
            ScalarValue::TimestampMillisecond(e, tz_opt) => build_array_from_option!(
                Timestamp,
                TimeUnit::Millisecond,
                tz_opt.clone(),
                TimestampMillisecondArray,
                e,
                size
            ),

            ScalarValue::TimestampMicrosecond(e, tz_opt) => build_array_from_option!(
                Timestamp,
                TimeUnit::Microsecond,
                tz_opt.clone(),
                TimestampMicrosecondArray,
                e,
                size
            ),
            ScalarValue::TimestampNanosecond(e, tz_opt) => build_array_from_option!(
                Timestamp,
                TimeUnit::Nanosecond,
                tz_opt.clone(),
                TimestampNanosecondArray,
                e,
                size
            ),
            ScalarValue::Utf8(e) => match e {
                Some(value) => {
                    Arc::new(StringArray::from_iter_values(repeat(value).take(size)))
                }
                None => new_null_array(&DataType::Utf8, size),
            },
            ScalarValue::LargeUtf8(e) => match e {
                Some(value) => {
                    Arc::new(LargeStringArray::from_iter_values(repeat(value).take(size)))
                }
                None => new_null_array(&DataType::LargeUtf8, size),
            },
            ScalarValue::Binary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<BinaryArray>(),
                ),
                None => {
                    Arc::new(repeat(None::<&str>).take(size).collect::<BinaryArray>())
                }
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(value) => Arc::new(
                    repeat(Some(value.as_slice()))
                        .take(size)
                        .collect::<LargeBinaryArray>(),
                ),
                None => Arc::new(
                    repeat(None::<&str>)
                        .take(size)
                        .collect::<LargeBinaryArray>(),
                ),
            },
            ScalarValue::List(values, field) => Arc::new(match field.data_type() {
                DataType::Boolean => build_list!(BooleanBuilder, Boolean, values, size),
                DataType::Int8 => build_list!(Int8Builder, Int8, values, size),
                DataType::Int16 => build_list!(Int16Builder, Int16, values, size),
                DataType::Int32 => build_list!(Int32Builder, Int32, values, size),
                DataType::Int64 => build_list!(Int64Builder, Int64, values, size),
                DataType::UInt8 => build_list!(UInt8Builder, UInt8, values, size),
                DataType::UInt16 => build_list!(UInt16Builder, UInt16, values, size),
                DataType::UInt32 => build_list!(UInt32Builder, UInt32, values, size),
                DataType::UInt64 => build_list!(UInt64Builder, UInt64, values, size),
                DataType::Utf8 => build_list!(StringBuilder, Utf8, values, size),
                DataType::Float32 => build_list!(Float32Builder, Float32, values, size),
                DataType::Float64 => build_list!(Float64Builder, Float64, values, size),
                DataType::Timestamp(unit, tz) => {
                    build_timestamp_list!(unit.clone(), tz.clone(), values, size)
                }
                &DataType::LargeUtf8 => {
                    build_list!(LargeStringBuilder, LargeUtf8, values, size)
                }
                _ => ScalarValue::iter_to_array_list(
                    repeat(self.clone()).take(size),
                    &DataType::List(Box::new(Field::new(
                        "item",
                        field.data_type().clone(),
                        true,
                    ))),
                )
                .unwrap(),
            }),
            ScalarValue::Date32(e) => {
                build_array_from_option!(Date32, Date32Array, e, size)
            }
            ScalarValue::Date64(e) => {
                build_array_from_option!(Date64, Date64Array, e, size)
            }
            ScalarValue::IntervalDayTime(e) => build_array_from_option!(
                Interval,
                IntervalUnit::DayTime,
                IntervalDayTimeArray,
                e,
                size
            ),
            ScalarValue::IntervalYearMonth(e) => build_array_from_option!(
                Interval,
                IntervalUnit::YearMonth,
                IntervalYearMonthArray,
                e,
                size
            ),
            ScalarValue::IntervalMonthDayNano(e) => build_array_from_option!(
                Interval,
                IntervalUnit::MonthDayNano,
                IntervalMonthDayNanoArray,
                e,
                size
            ),
            ScalarValue::Struct(values, fields) => match values {
                Some(values) => {
                    let field_values: Vec<_> = fields
                        .iter()
                        .zip(values.iter())
                        .map(|(field, value)| {
                            (field.clone(), value.to_array_of_size(size))
                        })
                        .collect();

                    Arc::new(StructArray::from(field_values))
                }
                None => {
                    let field_values: Vec<_> = fields
                        .iter()
                        .map(|field| {
                            let none_field = Self::try_from(field.data_type())
                .expect("Failed to construct null ScalarValue from Struct field type");
                            (field.clone(), none_field.to_array_of_size(size))
                        })
                        .collect();

                    Arc::new(StructArray::from(field_values))
                }
            },
            ScalarValue::Dictionary(key_type, v) => {
                // values array is one element long (the value)
                match key_type.as_ref() {
                    DataType::Int8 => dict_from_scalar::<Int8Type>(v, size),
                    DataType::Int16 => dict_from_scalar::<Int16Type>(v, size),
                    DataType::Int32 => dict_from_scalar::<Int32Type>(v, size),
                    DataType::Int64 => dict_from_scalar::<Int64Type>(v, size),
                    DataType::UInt8 => dict_from_scalar::<UInt8Type>(v, size),
                    DataType::UInt16 => dict_from_scalar::<UInt16Type>(v, size),
                    DataType::UInt32 => dict_from_scalar::<UInt32Type>(v, size),
                    DataType::UInt64 => dict_from_scalar::<UInt64Type>(v, size),
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            ScalarValue::Null => new_null_array(&DataType::Null, size),
        }
    }

    fn get_decimal_value_from_array(
        array: &ArrayRef,
        index: usize,
        precision: &usize,
        scale: &usize,
    ) -> ScalarValue {
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        if array.is_null(index) {
            ScalarValue::Decimal128(None, *precision, *scale)
        } else {
            ScalarValue::Decimal128(
                Some(array.value(index).as_i128()),
                *precision,
                *scale,
            )
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return array.data_type().try_into();
        }

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Decimal128(precision, scale) => {
                ScalarValue::get_decimal_value_from_array(array, index, precision, scale)
            }
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Binary => typed_cast!(array, index, BinaryArray, Binary),
            DataType::LargeBinary => {
                typed_cast!(array, index, LargeBinaryArray, LargeBinary)
            }
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            DataType::LargeUtf8 => typed_cast!(array, index, LargeStringArray, LargeUtf8),
            DataType::List(nested_type) => {
                let list_array =
                    array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Failed to downcast ListArray".to_string(),
                        )
                    })?;
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| ScalarValue::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                ScalarValue::List(
                    value,
                    Box::new(Field::new("item", nested_type.data_type().clone(), true)),
                )
            }
            DataType::Date32 => {
                typed_cast!(array, index, Date32Array, Date32)
            }
            DataType::Date64 => {
                typed_cast!(array, index, Date64Array, Date64)
            }
            DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                typed_cast_tz!(
                    array,
                    index,
                    TimestampSecondArray,
                    TimestampSecond,
                    tz_opt
                )
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                typed_cast_tz!(
                    array,
                    index,
                    TimestampMillisecondArray,
                    TimestampMillisecond,
                    tz_opt
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                typed_cast_tz!(
                    array,
                    index,
                    TimestampMicrosecondArray,
                    TimestampMicrosecond,
                    tz_opt
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                typed_cast_tz!(
                    array,
                    index,
                    TimestampNanosecondArray,
                    TimestampNanosecond,
                    tz_opt
                )
            }
            DataType::Dictionary(key_type, _) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index),
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index),
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index),
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index),
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index),
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index),
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index),
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index),
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                };
                // look up the index in the values dictionary
                let value = match values_index {
                    Some(values_index) => {
                        ScalarValue::try_from_array(values_array, values_index)
                    }
                    // else entry was null, so return null
                    None => values_array.data_type().try_into(),
                }?;

                Self::Dictionary(key_type.clone(), Box::new(value))
            }
            DataType::Struct(fields) => {
                let array =
                    array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .ok_or_else(|| {
                            DataFusionError::Internal(
                                "Failed to downcast ArrayRef to StructArray".to_string(),
                            )
                        })?;
                let mut field_values: Vec<ScalarValue> = Vec::new();
                for col_index in 0..array.num_columns() {
                    let col_array = array.column(col_index);
                    let col_scalar = ScalarValue::try_from_array(col_array, index)?;
                    field_values.push(col_scalar);
                }
                Self::Struct(Some(field_values), Box::new(fields.clone()))
            }
            DataType::FixedSizeList(nested_type, _len) => {
                let list_array =
                    array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array = list_array.value(index);
                        let scalar_vec = (0..nested_array.len())
                            .map(|i| ScalarValue::try_from_array(&nested_array, i))
                            .collect::<Result<Vec<_>>>()?;
                        Some(scalar_vec)
                    }
                };
                ScalarValue::List(
                    value,
                    Box::new(Field::new("item", nested_type.data_type().clone(), true)),
                )
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar from array of type \"{:?}\"",
                    other
                )));
            }
        })
    }

    /// Try to parse `value` into a ScalarValue of type `target_type`
    pub fn try_from_string(value: String, target_type: &DataType) -> Result<Self> {
        let value = ScalarValue::Utf8(Some(value));
        let cast_options = CastOptions { safe: false };
        let cast_arr = cast_with_options(&value.to_array(), target_type, &cast_options)?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }

    fn eq_array_decimal(
        array: &ArrayRef,
        index: usize,
        value: &Option<i128>,
        precision: usize,
        scale: usize,
    ) -> bool {
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        if array.precision() != precision || array.scale() != scale {
            return false;
        }
        match value {
            None => array.is_null(index),
            Some(v) => {
                !array.is_null(index)
                    && array.value(index)
                        == Decimal128::new(precision, scale, &v.to_le_bytes())
            }
        }
    }

    /// Compares a single row of array @ index for equality with self,
    /// in an optimized fashion.
    ///
    /// This method implements an optimized version of:
    ///
    /// ```text
    ///     let arr_scalar = Self::try_from_array(array, index).unwrap();
    ///     arr_scalar.eq(self)
    /// ```
    ///
    /// *Performance note*: the arrow compute kernels should be
    /// preferred over this function if at all possible as they can be
    /// vectorized and are generally much faster.
    ///
    /// This function has a few narrow usescases such as hash table key
    /// comparisons where comparing a single row at a time is necessary.
    #[inline]
    pub fn eq_array(&self, array: &ArrayRef, index: usize) -> bool {
        match self {
            ScalarValue::Decimal128(v, precision, scale) => {
                ScalarValue::eq_array_decimal(array, index, v, *precision, *scale)
            }
            ScalarValue::Boolean(val) => {
                eq_array_primitive!(array, index, BooleanArray, val)
            }
            ScalarValue::Float32(val) => {
                eq_array_primitive!(array, index, Float32Array, val)
            }
            ScalarValue::Float64(val) => {
                eq_array_primitive!(array, index, Float64Array, val)
            }
            ScalarValue::Int8(val) => eq_array_primitive!(array, index, Int8Array, val),
            ScalarValue::Int16(val) => eq_array_primitive!(array, index, Int16Array, val),
            ScalarValue::Int32(val) => eq_array_primitive!(array, index, Int32Array, val),
            ScalarValue::Int64(val) => eq_array_primitive!(array, index, Int64Array, val),
            ScalarValue::UInt8(val) => eq_array_primitive!(array, index, UInt8Array, val),
            ScalarValue::UInt16(val) => {
                eq_array_primitive!(array, index, UInt16Array, val)
            }
            ScalarValue::UInt32(val) => {
                eq_array_primitive!(array, index, UInt32Array, val)
            }
            ScalarValue::UInt64(val) => {
                eq_array_primitive!(array, index, UInt64Array, val)
            }
            ScalarValue::Utf8(val) => eq_array_primitive!(array, index, StringArray, val),
            ScalarValue::LargeUtf8(val) => {
                eq_array_primitive!(array, index, LargeStringArray, val)
            }
            ScalarValue::Binary(val) => {
                eq_array_primitive!(array, index, BinaryArray, val)
            }
            ScalarValue::LargeBinary(val) => {
                eq_array_primitive!(array, index, LargeBinaryArray, val)
            }
            ScalarValue::List(_, _) => unimplemented!(),
            ScalarValue::Date32(val) => {
                eq_array_primitive!(array, index, Date32Array, val)
            }
            ScalarValue::Date64(val) => {
                eq_array_primitive!(array, index, Date64Array, val)
            }
            ScalarValue::TimestampSecond(val, _) => {
                eq_array_primitive!(array, index, TimestampSecondArray, val)
            }
            ScalarValue::TimestampMillisecond(val, _) => {
                eq_array_primitive!(array, index, TimestampMillisecondArray, val)
            }
            ScalarValue::TimestampMicrosecond(val, _) => {
                eq_array_primitive!(array, index, TimestampMicrosecondArray, val)
            }
            ScalarValue::TimestampNanosecond(val, _) => {
                eq_array_primitive!(array, index, TimestampNanosecondArray, val)
            }
            ScalarValue::IntervalYearMonth(val) => {
                eq_array_primitive!(array, index, IntervalYearMonthArray, val)
            }
            ScalarValue::IntervalDayTime(val) => {
                eq_array_primitive!(array, index, IntervalDayTimeArray, val)
            }
            ScalarValue::IntervalMonthDayNano(val) => {
                eq_array_primitive!(array, index, IntervalMonthDayNanoArray, val)
            }
            ScalarValue::Struct(_, _) => unimplemented!(),
            ScalarValue::Dictionary(key_type, v) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index),
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index),
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index),
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index),
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index),
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index),
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index),
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index),
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                };
                // was the value in the array non null?
                match values_index {
                    Some(values_index) => v.eq_array(values_array, values_index),
                    None => v.is_null(),
                }
            }
            ScalarValue::Null => array.data().is_null(index),
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        Some(value).into()
    }
}

impl From<Option<&str>> for ScalarValue {
    fn from(value: Option<&str>) -> Self {
        let value = value.map(|s| s.to_string());
        ScalarValue::Utf8(value)
    }
}

impl FromStr for ScalarValue {
    type Err = Infallible;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl From<Vec<(&str, ScalarValue)>> for ScalarValue {
    fn from(value: Vec<(&str, ScalarValue)>) -> Self {
        let (fields, scalars): (Vec<_>, Vec<_>) = value
            .into_iter()
            .map(|(name, scalar)| {
                (Field::new(name, scalar.get_datatype(), false), scalar)
            })
            .unzip();

        Self::Struct(Some(scalars), Box::new(fields))
    }
}

macro_rules! impl_try_from {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<ScalarValue> for $NATIVE {
            type Error = DataFusionError;

            fn try_from(value: ScalarValue) -> Result<Self> {
                match value {
                    ScalarValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => Err(DataFusionError::Internal(format!(
                        "Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ))),
                }
            }
        }
    };
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);

// special implementation for i32 because of Date32
impl TryFrom<ScalarValue> for i32 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int32(Some(inner_value))
            | ScalarValue::Date32(Some(inner_value)) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

// special implementation for i64 because of TimeNanosecond
impl TryFrom<ScalarValue> for i64 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int64(Some(inner_value))
            | ScalarValue::Date64(Some(inner_value))
            | ScalarValue::TimestampNanosecond(Some(inner_value), _)
            | ScalarValue::TimestampMicrosecond(Some(inner_value), _)
            | ScalarValue::TimestampMillisecond(Some(inner_value), _)
            | ScalarValue::TimestampSecond(Some(inner_value), _) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

// special implementation for i128 because of Decimal128
impl TryFrom<ScalarValue> for i128 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Decimal128(Some(inner_value), _, _) => Ok(inner_value),
            _ => Err(DataFusionError::Internal(format!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ))),
        }
    }
}

impl_try_from!(UInt8, u8);
impl_try_from!(UInt16, u16);
impl_try_from!(UInt32, u32);
impl_try_from!(UInt64, u64);
impl_try_from!(Float32, f32);
impl_try_from!(Float64, f64);
impl_try_from!(Boolean, bool);

impl TryFrom<&DataType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(datatype: &DataType) -> Result<Self> {
        Ok(match datatype {
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Float32 => ScalarValue::Float32(None),
            DataType::Int8 => ScalarValue::Int8(None),
            DataType::Int16 => ScalarValue::Int16(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::UInt8 => ScalarValue::UInt8(None),
            DataType::UInt16 => ScalarValue::UInt16(None),
            DataType::UInt32 => ScalarValue::UInt32(None),
            DataType::UInt64 => ScalarValue::UInt64(None),
            DataType::Decimal128(precision, scale) => {
                ScalarValue::Decimal128(None, *precision, *scale)
            }
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Date32 => ScalarValue::Date32(None),
            DataType::Date64 => ScalarValue::Date64(None),
            DataType::Timestamp(TimeUnit::Second, tz_opt) => {
                ScalarValue::TimestampSecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                ScalarValue::TimestampMillisecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                ScalarValue::TimestampMicrosecond(None, tz_opt.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                ScalarValue::TimestampNanosecond(None, tz_opt.clone())
            }
            DataType::Dictionary(index_type, value_type) => ScalarValue::Dictionary(
                index_type.clone(),
                Box::new(value_type.as_ref().try_into()?),
            ),
            DataType::List(ref nested_type) => ScalarValue::List(
                None,
                Box::new(Field::new("item", nested_type.data_type().clone(), true)),
            ),
            DataType::Struct(fields) => {
                ScalarValue::Struct(None, Box::new(fields.clone()))
            }
            DataType::Null => ScalarValue::Null,
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Can't create a scalar from data_type \"{:?}\"",
                    datatype
                )));
            }
        })
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(v, p, s) => {
                write!(f, "{:?},{:?},{:?}", v, p, s)?;
            }
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::TimestampSecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampMillisecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampMicrosecond(e, _) => format_option!(f, e)?,
            ScalarValue::TimestampNanosecond(e, _) => format_option!(f, e)?,
            ScalarValue::Utf8(e) => format_option!(f, e)?,
            ScalarValue::LargeUtf8(e) => format_option!(f, e)?,
            ScalarValue::Binary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::LargeBinary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::List(e, _) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{}", v))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Date32(e) => format_option!(f, e)?,
            ScalarValue::Date64(e) => format_option!(f, e)?,
            ScalarValue::IntervalDayTime(e) => format_option!(f, e)?,
            ScalarValue::IntervalYearMonth(e) => format_option!(f, e)?,
            ScalarValue::IntervalMonthDayNano(e) => format_option!(f, e)?,
            ScalarValue::Struct(e, fields) => match e {
                Some(l) => write!(
                    f,
                    "{{{}}}",
                    l.iter()
                        .zip(fields.iter())
                        .map(|(value, field)| format!("{}:{}", field.name(), value))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Dictionary(_k, v) => write!(f, "{}", v)?,
            ScalarValue::Null => write!(f, "NULL")?,
        };
        Ok(())
    }
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(_, _, _) => write!(f, "Decimal128({})", self),
            ScalarValue::Boolean(_) => write!(f, "Boolean({})", self),
            ScalarValue::Float32(_) => write!(f, "Float32({})", self),
            ScalarValue::Float64(_) => write!(f, "Float64({})", self),
            ScalarValue::Int8(_) => write!(f, "Int8({})", self),
            ScalarValue::Int16(_) => write!(f, "Int16({})", self),
            ScalarValue::Int32(_) => write!(f, "Int32({})", self),
            ScalarValue::Int64(_) => write!(f, "Int64({})", self),
            ScalarValue::UInt8(_) => write!(f, "UInt8({})", self),
            ScalarValue::UInt16(_) => write!(f, "UInt16({})", self),
            ScalarValue::UInt32(_) => write!(f, "UInt32({})", self),
            ScalarValue::UInt64(_) => write!(f, "UInt64({})", self),
            ScalarValue::TimestampSecond(_, tz_opt) => {
                write!(f, "TimestampSecond({}, {:?})", self, tz_opt)
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                write!(f, "TimestampMillisecond({}, {:?})", self, tz_opt)
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                write!(f, "TimestampMicrosecond({}, {:?})", self, tz_opt)
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                write!(f, "TimestampNanosecond({}, {:?})", self, tz_opt)
            }
            ScalarValue::Utf8(None) => write!(f, "Utf8({})", self),
            ScalarValue::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
            ScalarValue::LargeUtf8(None) => write!(f, "LargeUtf8({})", self),
            ScalarValue::LargeUtf8(Some(_)) => write!(f, "LargeUtf8(\"{}\")", self),
            ScalarValue::Binary(None) => write!(f, "Binary({})", self),
            ScalarValue::Binary(Some(_)) => write!(f, "Binary(\"{}\")", self),
            ScalarValue::LargeBinary(None) => write!(f, "LargeBinary({})", self),
            ScalarValue::LargeBinary(Some(_)) => write!(f, "LargeBinary(\"{}\")", self),
            ScalarValue::List(_, _) => write!(f, "List([{}])", self),
            ScalarValue::Date32(_) => write!(f, "Date32(\"{}\")", self),
            ScalarValue::Date64(_) => write!(f, "Date64(\"{}\")", self),
            ScalarValue::IntervalDayTime(_) => {
                write!(f, "IntervalDayTime(\"{}\")", self)
            }
            ScalarValue::IntervalYearMonth(_) => {
                write!(f, "IntervalYearMonth(\"{}\")", self)
            }
            ScalarValue::IntervalMonthDayNano(_) => {
                write!(f, "IntervalMonthDayNano(\"{}\")", self)
            }
            ScalarValue::Struct(e, fields) => {
                // Use Debug representation of field values
                match e {
                    Some(l) => write!(
                        f,
                        "Struct({{{}}})",
                        l.iter()
                            .zip(fields.iter())
                            .map(|(value, field)| format!("{}:{:?}", field.name(), value))
                            .collect::<Vec<_>>()
                            .join(",")
                    ),
                    None => write!(f, "Struct(NULL)"),
                }
            }
            ScalarValue::Dictionary(k, v) => write!(f, "Dictionary({:?}, {:?})", k, v),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

/// Trait used to map a NativeTime to a ScalarType.
pub trait ScalarType<T: ArrowNativeType> {
    /// returns a scalar from an optional T
    fn scalar(r: Option<T>) -> ScalarValue;
}

impl ScalarType<f32> for Float32Type {
    fn scalar(r: Option<f32>) -> ScalarValue {
        ScalarValue::Float32(r)
    }
}

impl ScalarType<i64> for TimestampSecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampSecond(r, None)
    }
}

impl ScalarType<i64> for TimestampMillisecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMillisecond(r, None)
    }
}

impl ScalarType<i64> for TimestampMicrosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampMicrosecond(r, None)
    }
}

impl ScalarType<i64> for TimestampNanosecondType {
    fn scalar(r: Option<i64>) -> ScalarValue {
        ScalarValue::TimestampNanosecond(r, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::from_slice::FromSlice;
    use arrow::compute::kernels;
    use arrow::datatypes::ArrowPrimitiveType;
    use std::cmp::Ordering;
    use std::sync::Arc;

    #[test]
    fn scalar_decimal_test() {
        let decimal_value = ScalarValue::Decimal128(Some(123), 10, 1);
        assert_eq!(DataType::Decimal128(10, 1), decimal_value.get_datatype());
        let try_into_value: i128 = decimal_value.clone().try_into().unwrap();
        assert_eq!(123_i128, try_into_value);
        assert!(!decimal_value.is_null());
        let neg_decimal_value = decimal_value.arithmetic_negate();
        match neg_decimal_value {
            ScalarValue::Decimal128(v, _, _) => {
                assert_eq!(-123, v.unwrap());
            }
            _ => {
                unreachable!();
            }
        }

        // decimal scalar to array
        let array = decimal_value.to_array();
        let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(1, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array.value(0).as_i128());

        // decimal scalar to array with size
        let array = decimal_value.to_array_of_size(10);
        let array_decimal = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(10, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array_decimal.value(0).as_i128());
        assert_eq!(123i128, array_decimal.value(9).as_i128());
        // test eq array
        assert!(decimal_value.eq_array(&array, 1));
        assert!(decimal_value.eq_array(&array, 5));
        // test try from array
        assert_eq!(
            decimal_value,
            ScalarValue::try_from_array(&array, 5).unwrap()
        );

        assert_eq!(
            decimal_value,
            ScalarValue::try_new_decimal128(123, 10, 1).unwrap()
        );

        // test compare
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        assert!(!left.eq(&right));
        let result = left < right;
        assert!(result);
        let result = left <= right;
        assert!(result);
        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        // make sure that two decimals with diff datatype can't be compared.
        let result = left.partial_cmp(&right);
        assert_eq!(None, result);

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
        ];
        // convert the vec to decimal array and check the result
        let array = ScalarValue::iter_to_array(decimal_vec.into_iter()).unwrap();
        assert_eq!(3, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
        ];
        let array = ScalarValue::iter_to_array(decimal_vec.into_iter()).unwrap();
        assert_eq!(4, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        assert!(ScalarValue::try_new_decimal128(1, 10, 2)
            .unwrap()
            .eq_array(&array, 0));
        assert!(ScalarValue::try_new_decimal128(2, 10, 2)
            .unwrap()
            .eq_array(&array, 1));
        assert!(ScalarValue::try_new_decimal128(3, 10, 2)
            .unwrap()
            .eq_array(&array, 2));
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 3).unwrap()
        );
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 4).unwrap()
        );
    }

    #[test]
    fn scalar_value_to_array_u64() {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_value_to_array_u32() {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref = ScalarValue::List(
            None,
            Box::new(Field::new("item", DataType::UInt64, false)),
        )
        .to_array();
        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() {
        let list_array_ref = ScalarValue::List(
            Some(vec![
                ScalarValue::UInt64(Some(100)),
                ScalarValue::UInt64(None),
                ScalarValue::UInt64(Some(101)),
            ]),
            Box::new(Field::new("item", DataType::UInt64, false)),
        )
        .to_array();

        let list_array = list_array_ref.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = prim_array_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    macro_rules! check_scalar_iter {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> =
                $INPUT.iter().map(|v| ScalarValue::$SCALAR_T(*v)).collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    /// but for variants that carry a timezone field.
    macro_rules! check_scalar_iter_tz {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(*v, None))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for string  arrays
    macro_rules! check_scalar_iter_string {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_string())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: ArrayRef = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for binary arrays
    macro_rules! check_scalar_iter_binary {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_vec())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: $ARRAYTYPE =
                $INPUT.iter().map(|v| v.map(|v| v.to_vec())).collect();

            let expected: ArrayRef = Arc::new(expected);

            assert_eq!(&array, &expected);
        }};
    }

    #[test]
    fn scalar_iter_to_array_boolean() {
        check_scalar_iter!(Boolean, BooleanArray, vec![Some(true), None, Some(false)]);
        check_scalar_iter!(Float32, Float32Array, vec![Some(1.9), None, Some(-2.1)]);
        check_scalar_iter!(Float64, Float64Array, vec![Some(1.9), None, Some(-2.1)]);

        check_scalar_iter!(Int8, Int8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int16, Int16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int32, Int32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int64, Int64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter!(UInt8, UInt8Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt16, UInt16Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt32, UInt32Array, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt64, UInt64Array, vec![Some(1), None, Some(3)]);

        check_scalar_iter_tz!(
            TimestampSecond,
            TimestampSecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampMillisecond,
            TimestampMillisecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampMicrosecond,
            TimestampMicrosecondArray,
            vec![Some(1), None, Some(3)]
        );
        check_scalar_iter_tz!(
            TimestampNanosecond,
            TimestampNanosecondArray,
            vec![Some(1), None, Some(3)]
        );

        check_scalar_iter_string!(
            Utf8,
            StringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_string!(
            LargeUtf8,
            LargeStringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_binary!(
            Binary,
            BinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
        check_scalar_iter_binary!(
            LargeBinary,
            LargeBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
    }

    #[test]
    fn scalar_iter_to_array_empty() {
        let scalars = vec![] as Vec<ScalarValue>;

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("Empty iterator passed to ScalarValue::iter_to_array"),
            "{}",
            result
        );
    }

    #[test]
    fn scalar_iter_to_dictionary() {
        fn make_val(v: Option<String>) -> ScalarValue {
            let key_type = DataType::Int32;
            let value = ScalarValue::Utf8(v);
            ScalarValue::Dictionary(Box::new(key_type), Box::new(value))
        }

        let scalars = vec![
            make_val(Some("Foo".into())),
            make_val(None),
            make_val(Some("Bar".into())),
        ];

        let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();
        let array = as_dictionary_array::<Int32Type>(&array);
        let values_array = as_string_array(array.values());

        let values = array
            .keys_iter()
            .map(|k| {
                k.map(|k| {
                    assert!(values_array.is_valid(k));
                    values_array.value(k)
                })
            })
            .collect::<Vec<_>>();

        let expected = vec![Some("Foo"), None, Some("Bar")];
        assert_eq!(values, expected);
    }

    #[test]
    fn scalar_iter_to_array_mismatched_types() {
        use ScalarValue::*;
        // If the scalar values are not all the correct type, error here
        let scalars: Vec<ScalarValue> = vec![Boolean(Some(true)), Int32(Some(5))];

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(result.to_string().contains("Inconsistent types in ScalarValue::iter_to_array. Expected Boolean, got Int32(5)"),
                "{}", result);
    }

    #[test]
    fn scalar_try_from_array_null() {
        let array = vec![Some(33), None].into_iter().collect::<Int64Array>();
        let array: ArrayRef = Arc::new(array);

        assert_eq!(
            ScalarValue::Int64(Some(33)),
            ScalarValue::try_from_array(&array, 0).unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            ScalarValue::try_from_array(&array, 1).unwrap()
        );
    }

    #[test]
    fn scalar_try_from_dict_datatype() {
        let data_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let data_type = &data_type;
        let expected = ScalarValue::Dictionary(
            Box::new(DataType::Int8),
            Box::new(ScalarValue::Utf8(None)),
        );
        assert_eq!(expected, data_type.try_into().unwrap())
    }

    #[test]
    fn size_of_scalar() {
        // Since ScalarValues are used in a non trivial number of places,
        // making it larger means significant more memory consumption
        // per distinct value.
        //
        // The alignment requirements differ across architectures and
        // thus the size of the enum appears to as as well

        let expected = match cfg!(target_arch = "aarch64") {
            true => 64,
            false => 48,
        };

        assert_eq!(std::mem::size_of::<ScalarValue>(), expected);
    }

    #[test]
    fn scalar_eq_array() {
        // Validate that eq_array has the same semantics as ScalarValue::eq
        macro_rules! make_typed_vec {
            ($INPUT:expr, $TYPE:ident) => {{
                $INPUT
                    .iter()
                    .map(|v| v.map(|v| v as $TYPE))
                    .collect::<Vec<_>>()
            }};
        }

        let bool_vals = vec![Some(true), None, Some(false)];
        let f32_vals = vec![Some(-1.0), None, Some(1.0)];
        let f64_vals = make_typed_vec!(f32_vals, f64);

        let i8_vals = vec![Some(-1), None, Some(1)];
        let i16_vals = make_typed_vec!(i8_vals, i16);
        let i32_vals = make_typed_vec!(i8_vals, i32);
        let i64_vals = make_typed_vec!(i8_vals, i64);

        let u8_vals = vec![Some(0), None, Some(1)];
        let u16_vals = make_typed_vec!(u8_vals, u16);
        let u32_vals = make_typed_vec!(u8_vals, u32);
        let u64_vals = make_typed_vec!(u8_vals, u64);

        let str_vals = vec![Some("foo"), None, Some("bar")];

        /// Test each value in `scalar` with the corresponding element
        /// at `array`. Assumes each element is unique (aka not equal
        /// with all other indexes)
        #[derive(Debug)]
        struct TestCase {
            array: ArrayRef,
            scalars: Vec<ScalarValue>,
        }

        /// Create a test case for casing the input to the specified array type
        macro_rules! make_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};

            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                let tz = $TZ;
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(*v, tz.clone()))
                        .collect(),
                }
            }};
        }

        macro_rules! make_str_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.to_string())))
                        .collect(),
                }
            }};
        }

        macro_rules! make_binary_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::$SCALAR_TY(v.map(|v| v.as_bytes().to_vec()))
                        })
                        .collect(),
                }
            }};
        }

        /// create a test case for DictionaryArray<$INDEX_TY>
        macro_rules! make_str_dict_test_case {
            ($INPUT:expr, $INDEX_TY:ident) => {{
                TestCase {
                    array: Arc::new(
                        $INPUT
                            .iter()
                            .cloned()
                            .collect::<DictionaryArray<$INDEX_TY>>(),
                    ),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::Dictionary(
                                Box::new($INDEX_TY::DATA_TYPE),
                                Box::new(ScalarValue::Utf8(v.map(|v| v.to_string()))),
                            )
                        })
                        .collect(),
                }
            }};
        }

        let cases = vec![
            make_test_case!(bool_vals, BooleanArray, Boolean),
            make_test_case!(f32_vals, Float32Array, Float32),
            make_test_case!(f64_vals, Float64Array, Float64),
            make_test_case!(i8_vals, Int8Array, Int8),
            make_test_case!(i16_vals, Int16Array, Int16),
            make_test_case!(i32_vals, Int32Array, Int32),
            make_test_case!(i64_vals, Int64Array, Int64),
            make_test_case!(u8_vals, UInt8Array, UInt8),
            make_test_case!(u16_vals, UInt16Array, UInt16),
            make_test_case!(u32_vals, UInt32Array, UInt32),
            make_test_case!(u64_vals, UInt64Array, UInt64),
            make_str_test_case!(str_vals, StringArray, Utf8),
            make_str_test_case!(str_vals, LargeStringArray, LargeUtf8),
            make_binary_test_case!(str_vals, BinaryArray, Binary),
            make_binary_test_case!(str_vals, LargeBinaryArray, LargeBinary),
            make_test_case!(i32_vals, Date32Array, Date32),
            make_test_case!(i64_vals, Date64Array, Date64),
            make_test_case!(i64_vals, TimestampSecondArray, TimestampSecond, None),
            make_test_case!(
                i64_vals,
                TimestampSecondArray,
                TimestampSecond,
                Some("UTC".to_owned())
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMillisecondArray,
                TimestampMillisecond,
                Some("UTC".to_owned())
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                Some("UTC".to_owned())
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                None
            ),
            make_test_case!(
                i64_vals,
                TimestampNanosecondArray,
                TimestampNanosecond,
                Some("UTC".to_owned())
            ),
            make_test_case!(i32_vals, IntervalYearMonthArray, IntervalYearMonth),
            make_test_case!(i64_vals, IntervalDayTimeArray, IntervalDayTime),
            make_str_dict_test_case!(str_vals, Int8Type),
            make_str_dict_test_case!(str_vals, Int16Type),
            make_str_dict_test_case!(str_vals, Int32Type),
            make_str_dict_test_case!(str_vals, Int64Type),
            make_str_dict_test_case!(str_vals, UInt8Type),
            make_str_dict_test_case!(str_vals, UInt16Type),
            make_str_dict_test_case!(str_vals, UInt32Type),
            make_str_dict_test_case!(str_vals, UInt64Type),
        ];

        for case in cases {
            println!("**** Test Case *****");
            let TestCase { array, scalars } = case;
            println!("Input array type: {}", array.data_type());
            println!("Input scalars: {:#?}", scalars);
            assert_eq!(array.len(), scalars.len());

            for (index, scalar) in scalars.into_iter().enumerate() {
                assert!(
                    scalar.eq_array(&array, index),
                    "Expected {:?} to be equal to {:?} at index {}",
                    scalar,
                    array,
                    index
                );

                // test that all other elements are *not* equal
                for other_index in 0..array.len() {
                    if index != other_index {
                        assert!(
                            !scalar.eq_array(&array, other_index),
                            "Expected {:?} to be NOT equal to {:?} at index {}",
                            scalar,
                            array,
                            other_index
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_partial_ordering() {
        use ScalarValue::*;

        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(0))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Int64(Some(0)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Less)
        );
        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Equal)
        );
        // For different data type, `partial_cmp` returns None.
        assert_eq!(Int64(Some(33)).partial_cmp(&Int32(Some(33))), None);
        assert_eq!(Int32(Some(33)).partial_cmp(&Int64(Some(33))), None);

        assert_eq!(
            List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Equal)
        );

        assert_eq!(
            List(
                Some(vec![Int32(Some(10)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Greater)
        );

        assert_eq!(
            List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(10)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )),
            Some(Ordering::Less)
        );

        // For different data type, `partial_cmp` returns None.
        assert_eq!(
            List(
                Some(vec![Int64(Some(1)), Int64(Some(5))]),
                Box::new(Field::new("item", DataType::Int64, false)),
            )
            .partial_cmp(&List(
                Some(vec![Int32(Some(1)), Int32(Some(5))]),
                Box::new(Field::new("item", DataType::Int32, false)),
            )),
            None
        );

        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("A", ScalarValue::from(2.0)),
                ("B", ScalarValue::from("A")),
            ])),
            Some(Ordering::Less)
        );

        // For different struct fields, `partial_cmp` returns None.
        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("a", ScalarValue::from(2.0)),
                ("b", ScalarValue::from("A")),
            ])),
            None
        );
    }

    #[test]
    fn test_scalar_struct() {
        let field_a = Field::new("A", DataType::Int32, false);
        let field_b = Field::new("B", DataType::Boolean, false);
        let field_c = Field::new("C", DataType::Utf8, false);

        let field_e = Field::new("e", DataType::Int16, false);
        let field_f = Field::new("f", DataType::Int64, false);
        let field_d = Field::new(
            "D",
            DataType::Struct(vec![field_e.clone(), field_f.clone()]),
            false,
        );

        let scalar = ScalarValue::Struct(
            Some(vec![
                ScalarValue::Int32(Some(23)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Utf8(Some("Hello".to_string())),
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ]),
            Box::new(vec![
                field_a.clone(),
                field_b.clone(),
                field_c.clone(),
                field_d.clone(),
            ]),
        );

        // Check Display
        assert_eq!(
            format!("{}", scalar),
            String::from("{A:23,B:false,C:Hello,D:{e:2,f:3}}")
        );

        // Check Debug
        assert_eq!(
            format!("{:?}", scalar),
            String::from(
                r#"Struct({A:Int32(23),B:Boolean(false),C:Utf8("Hello"),D:Struct({e:Int16(2),f:Int64(3)})})"#
            )
        );

        // Convert to length-2 array
        let array = scalar.to_array_of_size(2);

        let expected = Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from_slice(&[23, 23])) as ArrayRef,
            ),
            (
                field_b.clone(),
                Arc::new(BooleanArray::from_slice(&[false, false])) as ArrayRef,
            ),
            (
                field_c.clone(),
                Arc::new(StringArray::from_slice(&["Hello", "Hello"])) as ArrayRef,
            ),
            (
                field_d.clone(),
                Arc::new(StructArray::from(vec![
                    (
                        field_e.clone(),
                        Arc::new(Int16Array::from_slice(&[2, 2])) as ArrayRef,
                    ),
                    (
                        field_f.clone(),
                        Arc::new(Int64Array::from_slice(&[3, 3])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);

        // Construct from second element of ArrayRef
        let constructed = ScalarValue::try_from_array(&expected, 1).unwrap();
        assert_eq!(constructed, scalar);

        // None version
        let none_scalar = ScalarValue::try_from(array.data_type()).unwrap();
        assert!(none_scalar.is_null());
        assert_eq!(format!("{:?}", none_scalar), String::from("Struct(NULL)"));

        // Construct with convenience From<Vec<(&str, ScalarValue)>>
        let constructed = ScalarValue::from(vec![
            ("A", ScalarValue::from(23)),
            ("B", ScalarValue::from(false)),
            ("C", ScalarValue::from("Hello")),
            (
                "D",
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ),
        ]);
        assert_eq!(constructed, scalar);

        // Build Array from Vec of structs
        let scalars = vec![
            ScalarValue::from(vec![
                ("A", ScalarValue::from(23)),
                ("B", ScalarValue::from(false)),
                ("C", ScalarValue::from("Hello")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(2i16)),
                        ("f", ScalarValue::from(3i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(7)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("World")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(4i16)),
                        ("f", ScalarValue::from(5i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(-1000)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("!!!!!")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(6i16)),
                        ("f", ScalarValue::from(7i64)),
                    ]),
                ),
            ]),
        ];
        let array = ScalarValue::iter_to_array(scalars).unwrap();

        let expected = Arc::new(StructArray::from(vec![
            (
                field_a,
                Arc::new(Int32Array::from_slice(&[23, 7, -1000])) as ArrayRef,
            ),
            (
                field_b,
                Arc::new(BooleanArray::from_slice(&[false, true, true])) as ArrayRef,
            ),
            (
                field_c,
                Arc::new(StringArray::from_slice(&["Hello", "World", "!!!!!"]))
                    as ArrayRef,
            ),
            (
                field_d,
                Arc::new(StructArray::from(vec![
                    (
                        field_e,
                        Arc::new(Int16Array::from_slice(&[2, 4, 6])) as ArrayRef,
                    ),
                    (
                        field_f,
                        Arc::new(Int64Array::from_slice(&[3, 5, 7])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);
    }

    #[test]
    fn test_lists_in_struct() {
        let field_a = Field::new("A", DataType::Utf8, false);
        let field_primitive_list = Field::new(
            "primitive_list",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            false,
        );

        // Define primitive list scalars
        let l0 = ScalarValue::List(
            Some(vec![
                ScalarValue::from(1i32),
                ScalarValue::from(2i32),
                ScalarValue::from(3i32),
            ]),
            Box::new(Field::new("item", DataType::Int32, false)),
        );

        let l1 = ScalarValue::List(
            Some(vec![ScalarValue::from(4i32), ScalarValue::from(5i32)]),
            Box::new(Field::new("item", DataType::Int32, false)),
        );

        let l2 = ScalarValue::List(
            Some(vec![ScalarValue::from(6i32)]),
            Box::new(Field::new("item", DataType::Int32, false)),
        );

        // Define struct scalars
        let s0 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("First")))),
            ("primitive_list", l0),
        ]);

        let s1 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Second")))),
            ("primitive_list", l1),
        ]);

        let s2 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Third")))),
            ("primitive_list", l2),
        ]);

        // iter_to_array for struct scalars
        let array =
            ScalarValue::iter_to_array(vec![s0.clone(), s1.clone(), s2.clone()]).unwrap();
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();

        let expected = StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(StringArray::from_slice(&["First", "Second", "Third"]))
                    as ArrayRef,
            ),
            (
                field_primitive_list.clone(),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(1), Some(2), Some(3)]),
                    Some(vec![Some(4), Some(5)]),
                    Some(vec![Some(6)]),
                ])),
            ),
        ]);

        assert_eq!(array, &expected);

        // Define list-of-structs scalars
        let nl0 = ScalarValue::List(
            Some(vec![s0.clone(), s1.clone()]),
            Box::new(Field::new("item", s0.get_datatype(), true)),
        );

        let nl1 = ScalarValue::List(
            Some(vec![s2]),
            Box::new(Field::new("item", s0.get_datatype(), true)),
        );

        let nl2 = ScalarValue::List(
            Some(vec![s1]),
            Box::new(Field::new("item", s0.get_datatype(), true)),
        );
        // iter_to_array for list-of-struct
        let array = ScalarValue::iter_to_array(vec![nl0, nl1, nl2]).unwrap();
        let array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Construct expected array with array builders
        let field_a_builder = StringBuilder::new(4);
        let primitive_value_builder = Int32Array::builder(8);
        let field_primitive_list_builder = ListBuilder::new(primitive_value_builder);

        let element_builder = StructBuilder::new(
            vec![field_a, field_primitive_list],
            vec![
                Box::new(field_a_builder),
                Box::new(field_primitive_list_builder),
            ],
        );
        let mut list_builder = ListBuilder::new(element_builder);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("First");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(1);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(2);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(3);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Third");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(6);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        list_builder
            .values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("Second");
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(4);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .values()
            .append_value(5);
        list_builder
            .values()
            .field_builder::<ListBuilder<PrimitiveBuilder<Int32Type>>>(1)
            .unwrap()
            .append(true);
        list_builder.values().append(true);
        list_builder.append(true);

        let expected = list_builder.finish();

        assert_eq!(array, &expected);
    }

    #[test]
    fn test_nested_lists() {
        // Define inner list scalars
        let l1 = ScalarValue::List(
            Some(vec![
                ScalarValue::List(
                    Some(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ]),
                    Box::new(Field::new("item", DataType::Int32, true)),
                ),
                ScalarValue::List(
                    Some(vec![ScalarValue::from(4i32), ScalarValue::from(5i32)]),
                    Box::new(Field::new("item", DataType::Int32, true)),
                ),
            ]),
            Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
        );

        let l2 = ScalarValue::List(
            Some(vec![
                ScalarValue::List(
                    Some(vec![ScalarValue::from(6i32)]),
                    Box::new(Field::new("item", DataType::Int32, true)),
                ),
                ScalarValue::List(
                    Some(vec![ScalarValue::from(7i32), ScalarValue::from(8i32)]),
                    Box::new(Field::new("item", DataType::Int32, true)),
                ),
            ]),
            Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
        );

        let l3 = ScalarValue::List(
            Some(vec![ScalarValue::List(
                Some(vec![ScalarValue::from(9i32)]),
                Box::new(Field::new("item", DataType::Int32, true)),
            )]),
            Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
        );

        let array = ScalarValue::iter_to_array(vec![l1, l2, l3]).unwrap();
        let array = array.as_any().downcast_ref::<ListArray>().unwrap();

        // Construct expected array with array builders
        let inner_builder = Int32Array::builder(8);
        let middle_builder = ListBuilder::new(inner_builder);
        let mut outer_builder = ListBuilder::new(middle_builder);

        outer_builder.values().values().append_value(1);
        outer_builder.values().values().append_value(2);
        outer_builder.values().values().append_value(3);
        outer_builder.values().append(true);

        outer_builder.values().values().append_value(4);
        outer_builder.values().values().append_value(5);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(6);
        outer_builder.values().append(true);

        outer_builder.values().values().append_value(7);
        outer_builder.values().values().append_value(8);
        outer_builder.values().append(true);
        outer_builder.append(true);

        outer_builder.values().values().append_value(9);
        outer_builder.values().append(true);
        outer_builder.append(true);

        let expected = outer_builder.finish();

        assert_eq!(array, &expected);
    }

    #[test]
    fn scalar_timestamp_ns_utc_timezone() {
        let scalar = ScalarValue::TimestampNanosecond(
            Some(1599566400000000000),
            Some("UTC".to_owned()),
        );

        assert_eq!(
            scalar.get_datatype(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );

        let array = scalar.to_array();
        assert_eq!(array.len(), 1);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );

        let newscalar = ScalarValue::try_from_array(&array, 0).unwrap();
        assert_eq!(
            newscalar.get_datatype(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );
    }

    #[test]
    fn cast_round_trip() {
        check_scalar_cast(ScalarValue::Int8(Some(5)), DataType::Int16);
        check_scalar_cast(ScalarValue::Int8(None), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(Some(5.5)), DataType::Int16);

        check_scalar_cast(ScalarValue::Float64(None), DataType::Int16);

        check_scalar_cast(
            ScalarValue::Utf8(Some("foo".to_string())),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );

        check_scalar_cast(
            ScalarValue::Utf8(None),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        );
    }

    // mimics how casting work on scalar values by `casting` `scalar` to `desired_type`
    fn check_scalar_cast(scalar: ScalarValue, desired_type: DataType) {
        // convert from scalar --> Array to call cast
        let scalar_array = scalar.to_array();
        // cast the actual value
        let cast_array = kernels::cast::cast(&scalar_array, &desired_type).unwrap();

        // turn it back to a scalar
        let cast_scalar = ScalarValue::try_from_array(&cast_array, 0).unwrap();
        assert_eq!(cast_scalar.get_datatype(), desired_type);

        // Some time later the "cast" scalar is turned back into an array:
        let array = cast_scalar.to_array_of_size(10);

        // The datatype should be "Dictionary" but is actually Utf8!!!
        assert_eq!(array.data_type(), &desired_type)
    }
}
