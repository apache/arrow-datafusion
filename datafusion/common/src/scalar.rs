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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::convert::{Infallible, TryInto};
use std::str::FromStr;
use std::sync::Arc;
use std::{convert::TryFrom, fmt, iter::repeat};

use crate::cast::{
    as_decimal128_array, as_decimal256_array, as_dictionary_array,
    as_fixed_size_binary_array, as_fixed_size_list_array, as_struct_array,
};
use crate::error::{DataFusionError, Result, _internal_err, _not_impl_err};
use crate::hash_utils::create_hashes;
use crate::logical_type::{ExtensionType, LogicalType, NamedLogicalType};
use crate::utils::array_into_list_array;
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::kernels::numeric::*;
use arrow::datatypes::{i256, Fields, SchemaBuilder};
use arrow::{
    array::*,
    compute::kernels::cast::{cast_with_options, CastOptions},
    datatypes::{
        ArrowDictionaryKeyType, ArrowNativeType, DataType, Field, Float32Type,
        Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
        IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, TimeUnit,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
        DECIMAL128_MAX_PRECISION,
    },
};
use arrow_array::cast::as_list_array;
use arrow_array::{ArrowNativeTypeOp, Scalar};
use arrow_schema::FieldRef;

/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part to arrow's [`Array`].
///
/// See [datatypes](https://arrow.apache.org/docs/python/api/datatypes.html) for
/// details on datatypes and the [format](https://github.com/apache/arrow/blob/master/format/Schema.fbs#L354-L375)
/// for the definitive reference.
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
    /// 128bit decimal, using the i128 to represent the decimal, precision scale
    Decimal128(Option<i128>, u8, i8),
    /// 256bit decimal, using the i256 to represent the decimal, precision scale
    Decimal256(Option<i256>, u8, i8),
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
    /// fixed size binary
    FixedSizeBinary(i32, Option<Vec<u8>>),
    /// large binary
    LargeBinary(Option<Vec<u8>>),
    /// Fixed size list of nested ScalarValue
    Fixedsizelist(Option<Vec<ScalarValue>>, FieldRef, i32),
    /// Represents a single element of a [`ListArray`] as an [`ArrayRef`]
    List(ArrayRef),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Date stored as a signed 64bit int milliseconds since UNIX epoch 1970-01-01
    Date64(Option<i64>),
    /// Time stored as a signed 32bit int as seconds since midnight
    Time32Second(Option<i32>),
    /// Time stored as a signed 32bit int as milliseconds since midnight
    Time32Millisecond(Option<i32>),
    /// Time stored as a signed 64bit int as microseconds since midnight
    Time64Microsecond(Option<i64>),
    /// Time stored as a signed 64bit int as nanoseconds since midnight
    Time64Nanosecond(Option<i64>),
    /// Timestamp Second
    TimestampSecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Milliseconds
    TimestampMillisecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Microseconds
    TimestampMicrosecond(Option<i64>, Option<Arc<str>>),
    /// Timestamp Nanoseconds
    TimestampNanosecond(Option<i64>, Option<Arc<str>>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<i64>),
    /// A triple of the number of elapsed months, days, and nanoseconds.
    /// Months and days are encoded as 32-bit signed integers.
    /// Nanoseconds is encoded as a 64-bit signed integer (no leap seconds).
    IntervalMonthDayNano(Option<i128>),
    /// Duration in seconds
    DurationSecond(Option<i64>),
    /// Duration in milliseconds
    DurationMillisecond(Option<i64>),
    /// Duration in microseconds
    DurationMicrosecond(Option<i64>),
    /// Duration in nanoseconds
    DurationNanosecond(Option<i64>),
    /// struct of nested ScalarValue
    Struct(Option<Vec<ScalarValue>>, Fields),
    /// Dictionary type: index type and value
    Dictionary(Box<DataType>, Box<ScalarValue>),
}

// manual implementation of `PartialEq`
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
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                v1.eq(v2) && p1.eq(p2) && s1.eq(s2)
            }
            (Decimal256(_, _, _), _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.to_bits() == f2.to_bits(),
                _ => v1.eq(v2),
            },
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
            (FixedSizeBinary(_, v1), FixedSizeBinary(_, v2)) => v1.eq(v2),
            (FixedSizeBinary(_, _), _) => false,
            (LargeBinary(v1), LargeBinary(v2)) => v1.eq(v2),
            (LargeBinary(_), _) => false,
            (Fixedsizelist(v1, t1, l1), Fixedsizelist(v2, t2, l2)) => {
                v1.eq(v2) && t1.eq(t2) && l1.eq(l2)
            }
            (Fixedsizelist(_, _, _), _) => false,
            (List(v1), List(v2)) => v1.eq(v2),
            (List(_), _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (Date64(v1), Date64(v2)) => v1.eq(v2),
            (Date64(_), _) => false,
            (Time32Second(v1), Time32Second(v2)) => v1.eq(v2),
            (Time32Second(_), _) => false,
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1.eq(v2),
            (Time32Millisecond(_), _) => false,
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1.eq(v2),
            (Time64Microsecond(_), _) => false,
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1.eq(v2),
            (Time64Nanosecond(_), _) => false,
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.eq(v2),
            (TimestampSecond(_, _), _) => false,
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => v1.eq(v2),
            (TimestampMillisecond(_, _), _) => false,
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => v1.eq(v2),
            (TimestampMicrosecond(_, _), _) => false,
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => v1.eq(v2),
            (TimestampNanosecond(_, _), _) => false,
            (DurationSecond(v1), DurationSecond(v2)) => v1.eq(v2),
            (DurationSecond(_), _) => false,
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1.eq(v2),
            (DurationMillisecond(_), _) => false,
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1.eq(v2),
            (DurationMicrosecond(_), _) => false,
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1.eq(v2),
            (DurationNanosecond(_), _) => false,
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

// manual implementation of `PartialOrd`
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
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.partial_cmp(v2)
                } else {
                    // Two decimal values can be compared if they have the same precision and scale.
                    None
                }
            }
            (Decimal256(_, _, _), _) => None,
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => Some(f1.total_cmp(f2)),
                _ => v1.partial_cmp(v2),
            },
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
            (FixedSizeBinary(_, v1), FixedSizeBinary(_, v2)) => v1.partial_cmp(v2),
            (FixedSizeBinary(_, _), _) => None,
            (LargeBinary(v1), LargeBinary(v2)) => v1.partial_cmp(v2),
            (LargeBinary(_), _) => None,
            (Fixedsizelist(v1, t1, l1), Fixedsizelist(v2, t2, l2)) => {
                if t1.eq(t2) && l1.eq(l2) {
                    v1.partial_cmp(v2)
                } else {
                    None
                }
            }
            (Fixedsizelist(_, _, _), _) => None,
            (List(arr1), List(arr2)) => {
                if arr1.data_type() == arr2.data_type() {
                    let list_arr1 = as_list_array(arr1);
                    let list_arr2 = as_list_array(arr2);
                    if list_arr1.len() != list_arr2.len() {
                        return None;
                    }
                    for i in 0..list_arr1.len() {
                        let arr1 = list_arr1.value(i);
                        let arr2 = list_arr2.value(i);

                        let lt_res =
                            arrow::compute::kernels::cmp::lt(&arr1, &arr2).ok()?;
                        let eq_res =
                            arrow::compute::kernels::cmp::eq(&arr1, &arr2).ok()?;

                        for j in 0..lt_res.len() {
                            if lt_res.is_valid(j) && lt_res.value(j) {
                                return Some(Ordering::Less);
                            }
                            if eq_res.is_valid(j) && !eq_res.value(j) {
                                return Some(Ordering::Greater);
                            }
                        }
                    }
                    Some(Ordering::Equal)
                } else {
                    None
                }
            }
            (List(_), _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (Date64(v1), Date64(v2)) => v1.partial_cmp(v2),
            (Date64(_), _) => None,
            (Time32Second(v1), Time32Second(v2)) => v1.partial_cmp(v2),
            (Time32Second(_), _) => None,
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1.partial_cmp(v2),
            (Time32Millisecond(_), _) => None,
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1.partial_cmp(v2),
            (Time64Microsecond(_), _) => None,
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1.partial_cmp(v2),
            (Time64Nanosecond(_), _) => None,
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
            (DurationSecond(v1), DurationSecond(v2)) => v1.partial_cmp(v2),
            (DurationSecond(_), _) => None,
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1.partial_cmp(v2),
            (DurationMillisecond(_), _) => None,
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1.partial_cmp(v2),
            (DurationMicrosecond(_), _) => None,
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1.partial_cmp(v2),
            (DurationNanosecond(_), _) => None,
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

//Float wrapper over f32/f64. Just because we cannot build std::hash::Hash for floats directly we have to do it through type wrapper
struct Fl<T>(T);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl std::hash::Hash for Fl<$t> {
            #[inline]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                state.write(&<$i>::from_ne_bytes(self.0.to_ne_bytes()).to_ne_bytes())
            }
        })+
    };
}

hash_float_value!((f64, u64), (f32, u32));

// manual implementation of `Hash`
//
// # Panics
//
// Panics if there is an error when creating hash values for rows
impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Decimal128(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state)
            }
            Decimal256(v, p, s) => {
                v.hash(state);
                p.hash(state);
                s.hash(state)
            }
            Boolean(v) => v.hash(state),
            Float32(v) => v.map(Fl).hash(state),
            Float64(v) => v.map(Fl).hash(state),
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
            FixedSizeBinary(_, v) => v.hash(state),
            LargeBinary(v) => v.hash(state),
            Fixedsizelist(v, t, l) => {
                v.hash(state);
                t.hash(state);
                l.hash(state);
            }
            List(arr) => {
                let arrays = vec![arr.to_owned()];
                let hashes_buffer = &mut vec![0; arr.len()];
                let random_state = ahash::RandomState::with_seeds(0, 0, 0, 0);
                let hashes =
                    create_hashes(&arrays, &random_state, hashes_buffer).unwrap();
                // Hash back to std::hash::Hasher
                hashes.hash(state);
            }
            Date32(v) => v.hash(state),
            Date64(v) => v.hash(state),
            Time32Second(v) => v.hash(state),
            Time32Millisecond(v) => v.hash(state),
            Time64Microsecond(v) => v.hash(state),
            Time64Nanosecond(v) => v.hash(state),
            TimestampSecond(v, _) => v.hash(state),
            TimestampMillisecond(v, _) => v.hash(state),
            TimestampMicrosecond(v, _) => v.hash(state),
            TimestampNanosecond(v, _) => v.hash(state),
            DurationSecond(v) => v.hash(state),
            DurationMillisecond(v) => v.hash(state),
            DurationMicrosecond(v) => v.hash(state),
            DurationNanosecond(v) => v.hash(state),
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

/// Return a reference to the values array and the index into it for a
/// dictionary array
///
/// # Errors
///
/// Errors if the array cannot be downcasted to DictionaryArray
#[inline]
pub fn get_dict_value<K: ArrowDictionaryKeyType>(
    array: &dyn Array,
    index: usize,
) -> Result<(&ArrayRef, Option<usize>)> {
    let dict_array = as_dictionary_array::<K>(array)?;
    Ok((dict_array.values(), dict_array.key(index)))
}

/// Create a dictionary array representing `value` repeated `size`
/// times
fn dict_from_scalar<K: ArrowDictionaryKeyType>(
    value: &ScalarValue,
    size: usize,
) -> Result<ArrayRef> {
    // values array is one element long (the value)
    let values_array = value.to_array_of_size(1)?;

    // Create a key array with `size` elements, each of 0
    let key_array: PrimitiveArray<K> = std::iter::repeat(Some(K::default_value()))
        .take(size)
        .collect();

    // create a new DictionaryArray
    //
    // Note: this path could be made faster by using the ArrayData
    // APIs and skipping validation, if it every comes up in
    // performance traces.
    Ok(Arc::new(
        DictionaryArray::<K>::try_new(key_array, values_array)?, // should always be valid by construction above
    ))
}

/// Create a dictionary array representing all the values in values
fn dict_from_values<K: ArrowDictionaryKeyType>(
    values_array: ArrayRef,
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
    let dict_array = DictionaryArray::<K>::try_new(key_array, values_array)?;
    Ok(Arc::new(dict_array))
}

macro_rules! typed_cast_tz {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident, $TZ:expr) => {{
        use std::any::type_name;
        let array = $array
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast value to {}",
                    type_name::<$ARRAYTYPE>()
                ))
            })?;
        Ok::<ScalarValue, DataFusionError>(ScalarValue::$SCALAR(
            match array.is_null($index) {
                true => None,
                false => Some(array.value($index).into()),
            },
            $TZ.clone(),
        ))
    }};
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        use std::any::type_name;
        let array = $array
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast value to {}",
                    type_name::<$ARRAYTYPE>()
                ))
            })?;
        Ok::<ScalarValue, DataFusionError>(ScalarValue::$SCALAR(
            match array.is_null($index) {
                true => None,
                false => Some(array.value($index).into()),
            },
        ))
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
}

macro_rules! build_timestamp_array_from_option {
    ($TIME_UNIT:expr, $TZ:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {
        match $EXPR {
            Some(value) => {
                Arc::new($ARRAY_TYPE::from_value(*value, $SIZE).with_timezone_opt($TZ))
            }
            None => new_null_array(&DataType::Timestamp($TIME_UNIT, $TZ), $SIZE),
        }
    };
}

macro_rules! eq_array_primitive {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $VALUE:expr) => {{
        use std::any::type_name;
        let array = $array
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "could not cast value to {}",
                    type_name::<$ARRAYTYPE>()
                ))
            })?;
        let is_valid = array.is_valid($index);
        Ok::<bool, DataFusionError>(match $VALUE {
            Some(val) => is_valid && &array.value($index) == val,
            None => !is_valid,
        })
    }};
}

impl ScalarValue {
    /// Create a [`Result<ScalarValue>`] with the provided value and datatype
    ///
    /// # Panics
    ///
    /// Panics if d is not compatible with T
    pub fn new_primitive<T: ArrowPrimitiveType>(
        a: Option<T::Native>,
        d: &DataType,
    ) -> Result<Self> {
        match a {
            None => d.try_into(),
            Some(v) => {
                let array = PrimitiveArray::<T>::new(vec![v].into(), None)
                    .with_data_type(d.clone());
                Self::try_from_array(&array, 0)
            }
        }
    }

    /// Create a decimal Scalar from value/precision and scale.
    pub fn try_new_decimal128(value: i128, precision: u8, scale: i8) -> Result<Self> {
        // make sure the precision and scale is valid
        if precision <= DECIMAL128_MAX_PRECISION && scale.unsigned_abs() <= precision {
            return Ok(ScalarValue::Decimal128(Some(value), precision, scale));
        }
        _internal_err!(
            "Can not new a decimal type ScalarValue for precision {precision} and scale {scale}"
        )
    }

    /// Returns a [`ScalarValue::Utf8`] representing `val`
    pub fn new_utf8(val: impl Into<String>) -> Self {
        ScalarValue::Utf8(Some(val.into()))
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

    /// Create a zero value in the given type.
    pub fn new_zero(datatype: &DataType) -> Result<ScalarValue> {
        assert!(datatype.is_primitive());
        Ok(match datatype {
            DataType::Boolean => ScalarValue::Boolean(Some(false)),
            DataType::Int8 => ScalarValue::Int8(Some(0)),
            DataType::Int16 => ScalarValue::Int16(Some(0)),
            DataType::Int32 => ScalarValue::Int32(Some(0)),
            DataType::Int64 => ScalarValue::Int64(Some(0)),
            DataType::UInt8 => ScalarValue::UInt8(Some(0)),
            DataType::UInt16 => ScalarValue::UInt16(Some(0)),
            DataType::UInt32 => ScalarValue::UInt32(Some(0)),
            DataType::UInt64 => ScalarValue::UInt64(Some(0)),
            DataType::Float32 => ScalarValue::Float32(Some(0.0)),
            DataType::Float64 => ScalarValue::Float64(Some(0.0)),
            DataType::Timestamp(TimeUnit::Second, tz) => {
                ScalarValue::TimestampSecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                ScalarValue::TimestampMillisecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                ScalarValue::TimestampMicrosecond(Some(0), tz.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                ScalarValue::TimestampNanosecond(Some(0), tz.clone())
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(Some(0))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(Some(0))
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(Some(0))
            }
            DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(Some(0)),
            DataType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(Some(0))
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(Some(0))
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(Some(0))
            }
            _ => {
                return _not_impl_err!(
                    "Can't create a zero scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// Create an one value in the given type.
    pub fn new_one(datatype: &DataType) -> Result<ScalarValue> {
        assert!(datatype.is_primitive());
        Ok(match datatype {
            DataType::Int8 => ScalarValue::Int8(Some(1)),
            DataType::Int16 => ScalarValue::Int16(Some(1)),
            DataType::Int32 => ScalarValue::Int32(Some(1)),
            DataType::Int64 => ScalarValue::Int64(Some(1)),
            DataType::UInt8 => ScalarValue::UInt8(Some(1)),
            DataType::UInt16 => ScalarValue::UInt16(Some(1)),
            DataType::UInt32 => ScalarValue::UInt32(Some(1)),
            DataType::UInt64 => ScalarValue::UInt64(Some(1)),
            DataType::Float32 => ScalarValue::Float32(Some(1.0)),
            DataType::Float64 => ScalarValue::Float64(Some(1.0)),
            _ => {
                return _not_impl_err!(
                    "Can't create an one scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// Create a negative one value in the given type.
    pub fn new_negative_one(datatype: &DataType) -> Result<ScalarValue> {
        assert!(datatype.is_primitive());
        Ok(match datatype {
            DataType::Int8 | DataType::UInt8 => ScalarValue::Int8(Some(-1)),
            DataType::Int16 | DataType::UInt16 => ScalarValue::Int16(Some(-1)),
            DataType::Int32 | DataType::UInt32 => ScalarValue::Int32(Some(-1)),
            DataType::Int64 | DataType::UInt64 => ScalarValue::Int64(Some(-1)),
            DataType::Float32 => ScalarValue::Float32(Some(-1.0)),
            DataType::Float64 => ScalarValue::Float64(Some(-1.0)),
            _ => {
                return _not_impl_err!(
                    "Can't create a negative one scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    pub fn new_ten(datatype: &DataType) -> Result<ScalarValue> {
        assert!(datatype.is_primitive());
        Ok(match datatype {
            DataType::Int8 => ScalarValue::Int8(Some(10)),
            DataType::Int16 => ScalarValue::Int16(Some(10)),
            DataType::Int32 => ScalarValue::Int32(Some(10)),
            DataType::Int64 => ScalarValue::Int64(Some(10)),
            DataType::UInt8 => ScalarValue::UInt8(Some(10)),
            DataType::UInt16 => ScalarValue::UInt16(Some(10)),
            DataType::UInt32 => ScalarValue::UInt32(Some(10)),
            DataType::UInt64 => ScalarValue::UInt64(Some(10)),
            DataType::Float32 => ScalarValue::Float32(Some(10.0)),
            DataType::Float64 => ScalarValue::Float64(Some(10.0)),
            _ => {
                return _not_impl_err!(
                    "Can't create a negative one scalar from data_type \"{datatype:?}\""
                );
            }
        })
    }

    /// return the [`DataType`] of this `ScalarValue`
    pub fn logical_type(&self) -> LogicalType {
        match self {
            ScalarValue::Boolean(_) => LogicalType::Boolean,
            ScalarValue::UInt8(_) => LogicalType::UInt8,
            ScalarValue::UInt16(_) => LogicalType::UInt16,
            ScalarValue::UInt32(_) => LogicalType::UInt32,
            ScalarValue::UInt64(_) => LogicalType::UInt64,
            ScalarValue::Int8(_) => LogicalType::Int8,
            ScalarValue::Int16(_) => LogicalType::Int16,
            ScalarValue::Int32(_) => LogicalType::Int32,
            ScalarValue::Int64(_) => LogicalType::Int64,
            ScalarValue::Decimal128(_, precision, scale) => {
                LogicalType::Decimal128(*precision, *scale)
            }
            ScalarValue::Decimal256(_, precision, scale) => {
                LogicalType::Decimal256(*precision, *scale)
            }
            ScalarValue::TimestampSecond(_, tz_opt) => {
                LogicalType::Timestamp(TimeUnit::Second, tz_opt.clone())
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                LogicalType::Timestamp(TimeUnit::Millisecond, tz_opt.clone())
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                LogicalType::Timestamp(TimeUnit::Microsecond, tz_opt.clone())
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                LogicalType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
            }
            ScalarValue::Float32(_) => LogicalType::Float32,
            ScalarValue::Float64(_) => LogicalType::Float64,
            ScalarValue::Utf8(_) => LogicalType::Utf8,
            ScalarValue::LargeUtf8(_) => LogicalType::LargeUtf8,
            ScalarValue::Binary(_) => LogicalType::Binary,
            ScalarValue::FixedSizeBinary(sz, _) => LogicalType::FixedSizeBinary(*sz),
            ScalarValue::LargeBinary(_) => LogicalType::LargeBinary,
            ScalarValue::Fixedsizelist(_, field, length) => {
                LogicalType::FixedSizeList(Box::new(field.data_type().into()), *length)
            }
            ScalarValue::List(arr) => arr.data_type().into(),
            ScalarValue::Date32(_) => LogicalType::Date32,
            ScalarValue::Date64(_) => LogicalType::Date64,
            ScalarValue::Time32Second(_) => LogicalType::Time32(TimeUnit::Second),
            ScalarValue::Time32Millisecond(_) => {
                LogicalType::Time32(TimeUnit::Millisecond)
            }
            ScalarValue::Time64Microsecond(_) => {
                LogicalType::Time64(TimeUnit::Microsecond)
            }
            ScalarValue::Time64Nanosecond(_) => LogicalType::Time64(TimeUnit::Nanosecond),
            ScalarValue::IntervalYearMonth(_) => {
                LogicalType::Interval(IntervalUnit::YearMonth)
            }
            ScalarValue::IntervalDayTime(_) => {
                LogicalType::Interval(IntervalUnit::DayTime)
            }
            ScalarValue::IntervalMonthDayNano(_) => {
                LogicalType::Interval(IntervalUnit::MonthDayNano)
            }
            ScalarValue::DurationSecond(_) => LogicalType::Duration(TimeUnit::Second),
            ScalarValue::DurationMillisecond(_) => {
                LogicalType::Duration(TimeUnit::Millisecond)
            }
            ScalarValue::DurationMicrosecond(_) => {
                LogicalType::Duration(TimeUnit::Microsecond)
            }
            ScalarValue::DurationNanosecond(_) => {
                LogicalType::Duration(TimeUnit::Nanosecond)
            }
            ScalarValue::Struct(_, fields) => {
                let fields = fields
                    .iter()
                    .map(|f| {
                        Arc::new(NamedLogicalType::new(f.name(), f.data_type().into()))
                    })
                    .collect::<Vec<_>>();
                LogicalType::Struct(fields.into())
            }
            ScalarValue::Dictionary(_k, v) => v.data_type().into(),
            ScalarValue::Null => LogicalType::Null,
        }
    }

    /// return the [`DataType`] of this `ScalarValue`
    pub fn data_type(&self) -> DataType {
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
            ScalarValue::Decimal256(_, precision, scale) => {
                DataType::Decimal256(*precision, *scale)
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
            ScalarValue::FixedSizeBinary(sz, _) => DataType::FixedSizeBinary(*sz),
            ScalarValue::LargeBinary(_) => DataType::LargeBinary,
            ScalarValue::Fixedsizelist(_, field, length) => {
                DataType::FixedSizeList(field.clone(), *length)
            }
            ScalarValue::List(arr) => arr.data_type().to_owned(),
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Time32Second(_) => DataType::Time32(TimeUnit::Second),
            ScalarValue::Time32Millisecond(_) => DataType::Time32(TimeUnit::Millisecond),
            ScalarValue::Time64Microsecond(_) => DataType::Time64(TimeUnit::Microsecond),
            ScalarValue::Time64Nanosecond(_) => DataType::Time64(TimeUnit::Nanosecond),
            ScalarValue::IntervalYearMonth(_) => {
                DataType::Interval(IntervalUnit::YearMonth)
            }
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::IntervalMonthDayNano(_) => {
                DataType::Interval(IntervalUnit::MonthDayNano)
            }
            ScalarValue::DurationSecond(_) => DataType::Duration(TimeUnit::Second),
            ScalarValue::DurationMillisecond(_) => {
                DataType::Duration(TimeUnit::Millisecond)
            }
            ScalarValue::DurationMicrosecond(_) => {
                DataType::Duration(TimeUnit::Microsecond)
            }
            ScalarValue::DurationNanosecond(_) => {
                DataType::Duration(TimeUnit::Nanosecond)
            }
            ScalarValue::Struct(_, fields) => DataType::Struct(fields.clone()),
            ScalarValue::Dictionary(k, v) => {
                DataType::Dictionary(k.clone(), Box::new(v.data_type()))
            }
            ScalarValue::Null => DataType::Null,
        }
    }

    /// Getter for the `DataType` of the value.
    ///
    /// Suggest using  [`Self::data_type`] as a more standard API
    #[deprecated(since = "31.0.0", note = "use data_type instead")]
    pub fn get_datatype(&self) -> DataType {
        self.data_type()
    }

    /// Calculate arithmetic negation for a scalar value
    pub fn arithmetic_negate(&self) -> Result<Self> {
        match self {
            ScalarValue::Int8(None)
            | ScalarValue::Int16(None)
            | ScalarValue::Int32(None)
            | ScalarValue::Int64(None)
            | ScalarValue::Float32(None)
            | ScalarValue::Float64(None) => Ok(self.clone()),
            ScalarValue::Float64(Some(v)) => Ok(ScalarValue::Float64(Some(-v))),
            ScalarValue::Float32(Some(v)) => Ok(ScalarValue::Float32(Some(-v))),
            ScalarValue::Int8(Some(v)) => Ok(ScalarValue::Int8(Some(-v))),
            ScalarValue::Int16(Some(v)) => Ok(ScalarValue::Int16(Some(-v))),
            ScalarValue::Int32(Some(v)) => Ok(ScalarValue::Int32(Some(-v))),
            ScalarValue::Int64(Some(v)) => Ok(ScalarValue::Int64(Some(-v))),
            ScalarValue::IntervalYearMonth(Some(v)) => {
                Ok(ScalarValue::IntervalYearMonth(Some(-v)))
            }
            ScalarValue::IntervalDayTime(Some(v)) => {
                let (days, ms) = IntervalDayTimeType::to_parts(*v);
                let val = IntervalDayTimeType::make_value(-days, -ms);
                Ok(ScalarValue::IntervalDayTime(Some(val)))
            }
            ScalarValue::IntervalMonthDayNano(Some(v)) => {
                let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
                let val = IntervalMonthDayNanoType::make_value(-months, -days, -nanos);
                Ok(ScalarValue::IntervalMonthDayNano(Some(val)))
            }
            ScalarValue::Decimal128(Some(v), precision, scale) => {
                Ok(ScalarValue::Decimal128(Some(-v), *precision, *scale))
            }
            ScalarValue::Decimal256(Some(v), precision, scale) => Ok(
                ScalarValue::Decimal256(Some(v.neg_wrapping()), *precision, *scale),
            ),
            value => _internal_err!(
                "Can not run arithmetic negative on scalar value {value:?}"
            ),
        }
    }

    /// Wrapping addition of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn add<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = add_wrapping(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }
    /// Checked addition of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn add_checked<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = add(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Wrapping subtraction of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn sub<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = sub_wrapping(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    /// Checked subtraction of `ScalarValue`
    ///
    /// NB: operating on `ScalarValue` directly is not efficient, performance sensitive code
    /// should operate on Arrays directly, using vectorized array kernels
    pub fn sub_checked<T: Borrow<ScalarValue>>(&self, other: T) -> Result<ScalarValue> {
        let r = sub(&self.to_scalar()?, &other.borrow().to_scalar()?)?;
        Self::try_from_array(r.as_ref(), 0)
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
        )
    }

    /// whether this value is null or not.
    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Boolean(v) => v.is_none(),
            ScalarValue::Null => true,
            ScalarValue::Float32(v) => v.is_none(),
            ScalarValue::Float64(v) => v.is_none(),
            ScalarValue::Decimal128(v, _, _) => v.is_none(),
            ScalarValue::Decimal256(v, _, _) => v.is_none(),
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
            ScalarValue::FixedSizeBinary(_, v) => v.is_none(),
            ScalarValue::LargeBinary(v) => v.is_none(),
            ScalarValue::Fixedsizelist(v, ..) => v.is_none(),
            ScalarValue::List(arr) => arr.len() == arr.null_count(),
            ScalarValue::Date32(v) => v.is_none(),
            ScalarValue::Date64(v) => v.is_none(),
            ScalarValue::Time32Second(v) => v.is_none(),
            ScalarValue::Time32Millisecond(v) => v.is_none(),
            ScalarValue::Time64Microsecond(v) => v.is_none(),
            ScalarValue::Time64Nanosecond(v) => v.is_none(),
            ScalarValue::TimestampSecond(v, _) => v.is_none(),
            ScalarValue::TimestampMillisecond(v, _) => v.is_none(),
            ScalarValue::TimestampMicrosecond(v, _) => v.is_none(),
            ScalarValue::TimestampNanosecond(v, _) => v.is_none(),
            ScalarValue::IntervalYearMonth(v) => v.is_none(),
            ScalarValue::IntervalDayTime(v) => v.is_none(),
            ScalarValue::IntervalMonthDayNano(v) => v.is_none(),
            ScalarValue::DurationSecond(v) => v.is_none(),
            ScalarValue::DurationMillisecond(v) => v.is_none(),
            ScalarValue::DurationMicrosecond(v) => v.is_none(),
            ScalarValue::DurationNanosecond(v) => v.is_none(),
            ScalarValue::Struct(v, _) => v.is_none(),
            ScalarValue::Dictionary(_, v) => v.is_null(),
        }
    }

    /// Absolute distance between two numeric values (of the same type). This method will return
    /// None if either one of the arguments are null. It might also return None if the resulting
    /// distance is greater than [`usize::MAX`]. If the type is a float, then the distance will be
    /// rounded to the nearest integer.
    ///
    ///
    /// Note: the datatype itself must support subtraction.
    pub fn distance(&self, other: &ScalarValue) -> Option<usize> {
        match (self, other) {
            (Self::Int8(Some(l)), Self::Int8(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int16(Some(l)), Self::Int16(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int32(Some(l)), Self::Int32(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::Int64(Some(l)), Self::Int64(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt8(Some(l)), Self::UInt8(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt16(Some(l)), Self::UInt16(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt32(Some(l)), Self::UInt32(Some(r))) => Some(l.abs_diff(*r) as _),
            (Self::UInt64(Some(l)), Self::UInt64(Some(r))) => Some(l.abs_diff(*r) as _),
            // TODO: we might want to look into supporting ceil/floor here for floats.
            (Self::Float32(Some(l)), Self::Float32(Some(r))) => {
                Some((l - r).abs().round() as _)
            }
            (Self::Float64(Some(l)), Self::Float64(Some(r))) => {
                Some((l - r).abs().round() as _)
            }
            _ => None,
        }
    }

    /// Converts a scalar value into an 1-row array.
    ///
    /// # Errors
    ///
    /// Errors if the ScalarValue cannot be converted into a 1-row array
    pub fn to_array(&self) -> Result<ArrayRef> {
        self.to_array_of_size(1)
    }

    /// Converts a scalar into an arrow [`Scalar`] (which implements
    /// the [`Datum`] interface).
    ///
    /// This can be used to call arrow compute kernels such as `lt`
    ///
    /// # Errors
    ///
    /// Errors if the ScalarValue cannot be converted into a 1-row array
    ///
    /// # Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{BooleanArray, Int32Array};
    ///
    /// let arr = Int32Array::from(vec![Some(1), None, Some(10)]);
    /// let five = ScalarValue::Int32(Some(5));
    ///
    /// let result = arrow::compute::kernels::cmp::lt(
    ///   &arr,
    ///   &five.to_scalar().unwrap(),
    /// ).unwrap();
    ///
    /// let expected = BooleanArray::from(vec![
    ///     Some(true),
    ///     None,
    ///     Some(false)
    ///   ]
    /// );
    ///
    /// assert_eq!(&result, &expected);
    /// ```
    /// [`Datum`]: arrow_array::Datum
    pub fn to_scalar(&self) -> Result<Scalar<ArrayRef>> {
        Ok(Scalar::new(self.to_array_of_size(1)?))
    }

    /// Converts an iterator of references [`ScalarValue`] into an [`ArrayRef`]
    /// corresponding to those values. For example, an iterator of
    /// [`ScalarValue::Int32`] would be converted to an [`Int32Array`].
    ///
    /// Returns an error if the iterator is empty or if the
    /// [`ScalarValue`]s are not all the same type
    ///
    /// # Panics
    ///
    /// Panics if `self` is a dictionary with invalid key type
    ///
    /// # Example
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
                return _internal_err!(
                    "Empty iterator passed to ScalarValue::iter_to_array"
                );
            }
            Some(sv) => sv.data_type(),
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
                            _internal_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        macro_rules! build_array_primitive_tz {
            ($ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                {
                    let array = scalars.map(|sv| {
                        if let ScalarValue::$SCALAR_TY(v, _) = sv {
                            Ok(v)
                        } else {
                            _internal_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array.with_timezone_opt($TZ.clone()))
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
                            _internal_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected {:?}, got {:?}",
                                data_type, sv
                            )
                        }
                    })
                    .collect::<Result<$ARRAY_TY>>()?;
                    Arc::new(array)
                }
            }};
        }

        macro_rules! build_array_list_primitive {
            ($ARRAY_TY:ident, $SCALAR_TY:ident, $NATIVE_TYPE:ident) => {{
                Ok::<ArrayRef, DataFusionError>(Arc::new(ListArray::from_iter_primitive::<$ARRAY_TY, _, _>(
                    scalars.into_iter().map(|x| match x {
                        ScalarValue::List(arr) => {
                            // `ScalarValue::List` contains a single element `ListArray`.
                            let list_arr = as_list_array(&arr);
                            if list_arr.is_null(0) {
                                Ok(None)
                            } else {
                                let primitive_arr =
                                    list_arr.values().as_primitive::<$ARRAY_TY>();
                                Ok(Some(
                                    primitive_arr.into_iter().collect::<Vec<Option<_>>>(),
                                ))
                            }
                        }
                        sv => _internal_err!(
                            "Inconsistent types in ScalarValue::iter_to_array. \
                                Expected {:?}, got {:?}",
                            data_type, sv
                        ),
                    })
                    .collect::<Result<Vec<_>>>()?,
                )))
            }};
        }

        macro_rules! build_array_list_string {
            ($BUILDER:ident, $STRING_ARRAY:ident) => {{
                let mut builder = ListBuilder::new($BUILDER::new());
                for scalar in scalars.into_iter() {
                    match scalar {
                        ScalarValue::List(arr) => {
                            // `ScalarValue::List` contains a single element `ListArray`.
                            let list_arr = as_list_array(&arr);

                            if list_arr.is_null(0) {
                                builder.append(false);
                                continue;
                            }

                            let string_arr = $STRING_ARRAY(list_arr.values());

                            for v in string_arr.iter() {
                                if let Some(v) = v {
                                    builder.values().append_value(v);
                                } else {
                                    builder.values().append_null();
                                }
                            }
                            builder.append(true);
                        }
                        sv => {
                            return _internal_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                    Expected List, got {:?}",
                                sv
                            )
                        }
                    }
                }
                Arc::new(builder.finish())
            }};
        }

        let array: ArrayRef = match &data_type {
            DataType::Decimal128(precision, scale) => {
                let decimal_array =
                    ScalarValue::iter_to_decimal_array(scalars, *precision, *scale)?;
                Arc::new(decimal_array)
            }
            DataType::Decimal256(precision, scale) => {
                let decimal_array =
                    ScalarValue::iter_to_decimal256_array(scalars, *precision, *scale)?;
                Arc::new(decimal_array)
            }
            DataType::Null => ScalarValue::iter_to_null_array(scalars)?,
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
            DataType::Time32(TimeUnit::Second) => {
                build_array_primitive!(Time32SecondArray, Time32Second)
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                build_array_primitive!(Time32MillisecondArray, Time32Millisecond)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                build_array_primitive!(Time64MicrosecondArray, Time64Microsecond)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                build_array_primitive!(Time64NanosecondArray, Time64Nanosecond)
            }
            DataType::Timestamp(TimeUnit::Second, tz) => {
                build_array_primitive_tz!(TimestampSecondArray, TimestampSecond, tz)
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                build_array_primitive_tz!(
                    TimestampMillisecondArray,
                    TimestampMillisecond,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                build_array_primitive_tz!(
                    TimestampMicrosecondArray,
                    TimestampMicrosecond,
                    tz
                )
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                build_array_primitive_tz!(
                    TimestampNanosecondArray,
                    TimestampNanosecond,
                    tz
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                build_array_primitive!(IntervalDayTimeArray, IntervalDayTime)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                build_array_primitive!(IntervalYearMonthArray, IntervalYearMonth)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                build_array_primitive!(IntervalMonthDayNanoArray, IntervalMonthDayNano)
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int8 => {
                build_array_list_primitive!(Int8Type, Int8, i8)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int16 => {
                build_array_list_primitive!(Int16Type, Int16, i16)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int32 => {
                build_array_list_primitive!(Int32Type, Int32, i32)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Int64 => {
                build_array_list_primitive!(Int64Type, Int64, i64)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt8 => {
                build_array_list_primitive!(UInt8Type, UInt8, u8)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt16 => {
                build_array_list_primitive!(UInt16Type, UInt16, u16)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt32 => {
                build_array_list_primitive!(UInt32Type, UInt32, u32)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::UInt64 => {
                build_array_list_primitive!(UInt64Type, UInt64, u64)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Float32 => {
                build_array_list_primitive!(Float32Type, Float32, f32)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Float64 => {
                build_array_list_primitive!(Float64Type, Float64, f64)?
            }
            DataType::List(fields) if fields.data_type() == &DataType::Utf8 => {
                build_array_list_string!(StringBuilder, as_string_array)
            }
            DataType::List(fields) if fields.data_type() == &DataType::LargeUtf8 => {
                build_array_list_string!(LargeStringBuilder, as_largestring_array)
            }
            DataType::List(_) => {
                // Fallback case handling homogeneous lists with any ScalarValue element type
                let list_array = ScalarValue::iter_to_array_list(scalars)?;
                Arc::new(list_array)
            }
            DataType::Struct(fields) => {
                // Initialize a Vector to store the ScalarValues for each column
                let mut columns: Vec<Vec<ScalarValue>> =
                    (0..fields.len()).map(|_| Vec::new()).collect();

                // null mask
                let mut null_mask_builder = BooleanBuilder::new();

                // Iterate over scalars to populate the column scalars for each row
                for scalar in scalars {
                    if let ScalarValue::Struct(values, fields) = scalar {
                        match values {
                            Some(values) => {
                                // Push value for each field
                                for (column, value) in columns.iter_mut().zip(values) {
                                    column.push(value.clone());
                                }
                                null_mask_builder.append_value(false);
                            }
                            None => {
                                // Push NULL of the appropriate type for each field
                                for (column, field) in
                                    columns.iter_mut().zip(fields.as_ref())
                                {
                                    column
                                        .push(ScalarValue::try_from(field.data_type())?);
                                }
                                null_mask_builder.append_value(true);
                            }
                        };
                    } else {
                        return _internal_err!("Expected Struct but found: {scalar}");
                    };
                }

                // Call iter_to_array recursively to convert the scalars for each column into Arrow arrays
                let field_values = fields
                    .iter()
                    .zip(columns)
                    .map(|(field, column)| {
                        Ok((field.clone(), Self::iter_to_array(column)?))
                    })
                    .collect::<Result<Vec<_>>>()?;

                let array = StructArray::from(field_values);
                arrow::compute::nullif(&array, &null_mask_builder.finish())?
            }
            DataType::Dictionary(key_type, value_type) => {
                // create the values array
                let value_scalars = scalars
                    .map(|scalar| match scalar {
                        ScalarValue::Dictionary(inner_key_type, scalar) => {
                            if &inner_key_type == key_type {
                                Ok(*scalar)
                            } else {
                                _internal_err!("Expected inner key type of {key_type} but found: {inner_key_type}, value was ({scalar:?})")
                            }
                        }
                        _ => {
                            _internal_err!(
                                "Expected scalar of type {value_type} but found: {scalar} {scalar:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let values = Self::iter_to_array(value_scalars)?;
                assert_eq!(values.data_type(), value_type.as_ref());

                match key_type.as_ref() {
                    DataType::Int8 => dict_from_values::<Int8Type>(values)?,
                    DataType::Int16 => dict_from_values::<Int16Type>(values)?,
                    DataType::Int32 => dict_from_values::<Int32Type>(values)?,
                    DataType::Int64 => dict_from_values::<Int64Type>(values)?,
                    DataType::UInt8 => dict_from_values::<UInt8Type>(values)?,
                    DataType::UInt16 => dict_from_values::<UInt16Type>(values)?,
                    DataType::UInt32 => dict_from_values::<UInt32Type>(values)?,
                    DataType::UInt64 => dict_from_values::<UInt64Type>(values)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            DataType::FixedSizeBinary(size) => {
                let array = scalars
                    .map(|sv| {
                        if let ScalarValue::FixedSizeBinary(_, v) = sv {
                            Ok(v)
                        } else {
                            _internal_err!(
                                "Inconsistent types in ScalarValue::iter_to_array. \
                                Expected {data_type:?}, got {sv:?}"
                            )
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
                let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    array.into_iter(),
                    *size,
                )?;
                Arc::new(array)
            }
            // explicitly enumerate unsupported types so newly added
            // types must be aknowledged, Time32 and Time64 types are
            // not supported if the TimeUnit is not valid (Time32 can
            // only be used with Second and Millisecond, Time64 only
            // with Microsecond and Nanosecond)
            DataType::Float16
            | DataType::Time32(TimeUnit::Microsecond)
            | DataType::Time32(TimeUnit::Nanosecond)
            | DataType::Time64(TimeUnit::Second)
            | DataType::Time64(TimeUnit::Millisecond)
            | DataType::Duration(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::Union(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _) => {
                return _internal_err!(
                    "Unsupported creation of {:?} array from ScalarValue {:?}",
                    data_type,
                    scalars.peek()
                );
            }
        };

        Ok(array)
    }

    fn iter_to_null_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<ArrayRef> {
        let length = scalars.into_iter().try_fold(
            0usize,
            |r, element: ScalarValue| match element {
                ScalarValue::Null => Ok::<usize, DataFusionError>(r + 1),
                s => {
                    _internal_err!("Expected ScalarValue::Null element. Received {s:?}")
                }
            },
        )?;
        Ok(new_null_array(&DataType::Null, length))
    }

    fn iter_to_decimal_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal128Array> {
        let array = scalars
            .into_iter()
            .map(|element: ScalarValue| match element {
                ScalarValue::Decimal128(v1, _, _) => Ok(v1),
                s => {
                    _internal_err!("Expected ScalarValue::Null element. Received {s:?}")
                }
            })
            .collect::<Result<Decimal128Array>>()?
            .with_precision_and_scale(precision, scale)?;
        Ok(array)
    }

    fn iter_to_decimal256_array(
        scalars: impl IntoIterator<Item = ScalarValue>,
        precision: u8,
        scale: i8,
    ) -> Result<Decimal256Array> {
        let array = scalars
            .into_iter()
            .map(|element: ScalarValue| match element {
                ScalarValue::Decimal256(v1, _, _) => Ok(v1),
                s => {
                    _internal_err!(
                        "Expected ScalarValue::Decimal256 element. Received {s:?}"
                    )
                }
            })
            .collect::<Result<Decimal256Array>>()?
            .with_precision_and_scale(precision, scale)?;
        Ok(array)
    }

    /// This function build with nulls with nulls buffer.
    fn iter_to_array_list(
        scalars: impl IntoIterator<Item = ScalarValue>,
    ) -> Result<GenericListArray<i32>> {
        let mut elements: Vec<ArrayRef> = vec![];
        let mut valid = BooleanBufferBuilder::new(0);
        let mut offsets = vec![];

        for scalar in scalars {
            if let ScalarValue::List(arr) = scalar {
                // `ScalarValue::List` contains a single element `ListArray`.
                let list_arr = as_list_array(&arr);

                if list_arr.is_null(0) {
                    // Repeat previous offset index
                    offsets.push(0);

                    // Element is null
                    valid.append(false);
                } else {
                    let arr = list_arr.values().to_owned();
                    offsets.push(arr.len());
                    elements.push(arr);

                    // Element is valid
                    valid.append(true);
                }
            } else {
                return _internal_err!(
                    "Expected ScalarValue::List element. Received {scalar:?}"
                );
            }
        }

        // Concatenate element arrays to create single flat array
        let element_arrays: Vec<&dyn Array> =
            elements.iter().map(|a| a.as_ref()).collect();

        let flat_array = match arrow::compute::concat(&element_arrays) {
            Ok(flat_array) => flat_array,
            Err(err) => return Err(DataFusionError::ArrowError(err)),
        };

        let buffer = valid.finish();

        let list_array = ListArray::new(
            Arc::new(Field::new("item", flat_array.data_type().clone(), true)),
            OffsetBuffer::<i32>::from_lengths(offsets),
            flat_array,
            Some(NullBuffer::new(buffer)),
        );

        Ok(list_array)
    }

    fn build_decimal_array(
        value: Option<i128>,
        precision: u8,
        scale: i8,
        size: usize,
    ) -> Result<Decimal128Array> {
        match value {
            Some(val) => Decimal128Array::from(vec![val; size])
                .with_precision_and_scale(precision, scale)
                .map_err(DataFusionError::ArrowError),
            None => {
                let mut builder = Decimal128Array::builder(size)
                    .with_precision_and_scale(precision, scale)
                    .map_err(DataFusionError::ArrowError)?;
                builder.append_nulls(size);
                Ok(builder.finish())
            }
        }
    }

    fn build_decimal256_array(
        value: Option<i256>,
        precision: u8,
        scale: i8,
        size: usize,
    ) -> Result<Decimal256Array> {
        std::iter::repeat(value)
            .take(size)
            .collect::<Decimal256Array>()
            .with_precision_and_scale(precision, scale)
            .map_err(DataFusionError::ArrowError)
    }

    /// Converts `Vec<ScalarValue>` where each element has type corresponding to
    /// `data_type`, to a [`ListArray`].
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::{ListArray, Int32Array};
    /// use arrow::datatypes::{DataType, Int32Type};
    /// use datafusion_common::cast::as_list_array;
    ///
    /// let scalars = vec![
    ///    ScalarValue::Int32(Some(1)),
    ///    ScalarValue::Int32(None),
    ///    ScalarValue::Int32(Some(2))
    /// ];
    ///
    /// let array = ScalarValue::new_list(&scalars, &DataType::Int32);
    /// let result = as_list_array(&array).unwrap();
    ///
    /// let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(
    ///     vec![
    ///        Some(vec![Some(1), None, Some(2)])
    ///     ]);
    ///
    /// assert_eq!(result, &expected);
    /// ```
    pub fn new_list(values: &[ScalarValue], data_type: &DataType) -> ArrayRef {
        let values = if values.is_empty() {
            new_empty_array(data_type)
        } else {
            Self::iter_to_array(values.iter().cloned()).unwrap()
        };
        Arc::new(array_into_list_array(values))
    }

    /// Converts a scalar value into an array of `size` rows.
    ///
    /// # Errors
    ///
    /// Errors if `self` is
    /// - a decimal that fails be converted to a decimal array of size
    /// - a `Fixedsizelist` that is not supported yet
    /// - a `List` that fails to be concatenated into an array of size
    /// - a `Dictionary` that fails be converted to a dictionary array of size
    pub fn to_array_of_size(&self, size: usize) -> Result<ArrayRef> {
        Ok(match self {
            ScalarValue::Decimal128(e, precision, scale) => Arc::new(
                ScalarValue::build_decimal_array(*e, *precision, *scale, size)?,
            ),
            ScalarValue::Decimal256(e, precision, scale) => Arc::new(
                ScalarValue::build_decimal256_array(*e, *precision, *scale, size)?,
            ),
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
            ScalarValue::TimestampSecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Second,
                    tz_opt.clone(),
                    TimestampSecondArray,
                    e,
                    size
                )
            }
            ScalarValue::TimestampMillisecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Millisecond,
                    tz_opt.clone(),
                    TimestampMillisecondArray,
                    e,
                    size
                )
            }

            ScalarValue::TimestampMicrosecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Microsecond,
                    tz_opt.clone(),
                    TimestampMicrosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::TimestampNanosecond(e, tz_opt) => {
                build_timestamp_array_from_option!(
                    TimeUnit::Nanosecond,
                    tz_opt.clone(),
                    TimestampNanosecondArray,
                    e,
                    size
                )
            }
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
            ScalarValue::FixedSizeBinary(s, e) => match e {
                Some(value) => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat(Some(value.as_slice())).take(size),
                        *s,
                    )
                    .unwrap(),
                ),
                None => Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                        repeat(None::<&[u8]>).take(size),
                        *s,
                    )
                    .unwrap(),
                ),
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
            ScalarValue::Fixedsizelist(..) => {
                return _not_impl_err!("FixedSizeList is not supported yet")
            }
            ScalarValue::List(arr) => {
                let arrays = std::iter::repeat(arr.as_ref())
                    .take(size)
                    .collect::<Vec<_>>();
                arrow::compute::concat(arrays.as_slice())
                    .map_err(DataFusionError::ArrowError)?
            }
            ScalarValue::Date32(e) => {
                build_array_from_option!(Date32, Date32Array, e, size)
            }
            ScalarValue::Date64(e) => {
                build_array_from_option!(Date64, Date64Array, e, size)
            }
            ScalarValue::Time32Second(e) => {
                build_array_from_option!(
                    Time32,
                    TimeUnit::Second,
                    Time32SecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time32Millisecond(e) => {
                build_array_from_option!(
                    Time32,
                    TimeUnit::Millisecond,
                    Time32MillisecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time64Microsecond(e) => {
                build_array_from_option!(
                    Time64,
                    TimeUnit::Microsecond,
                    Time64MicrosecondArray,
                    e,
                    size
                )
            }
            ScalarValue::Time64Nanosecond(e) => {
                build_array_from_option!(
                    Time64,
                    TimeUnit::Nanosecond,
                    Time64NanosecondArray,
                    e,
                    size
                )
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
            ScalarValue::DurationSecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Second,
                DurationSecondArray,
                e,
                size
            ),
            ScalarValue::DurationMillisecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Millisecond,
                DurationMillisecondArray,
                e,
                size
            ),
            ScalarValue::DurationMicrosecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Microsecond,
                DurationMicrosecondArray,
                e,
                size
            ),
            ScalarValue::DurationNanosecond(e) => build_array_from_option!(
                Duration,
                TimeUnit::Nanosecond,
                DurationNanosecondArray,
                e,
                size
            ),
            ScalarValue::Struct(values, fields) => match values {
                Some(values) => {
                    let field_values = fields
                        .iter()
                        .zip(values.iter())
                        .map(|(field, value)| {
                            Ok((field.clone(), value.to_array_of_size(size)?))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Arc::new(StructArray::from(field_values))
                }
                None => {
                    let dt = self.data_type();
                    new_null_array(&dt, size)
                }
            },
            ScalarValue::Dictionary(key_type, v) => {
                // values array is one element long (the value)
                match key_type.as_ref() {
                    DataType::Int8 => dict_from_scalar::<Int8Type>(v, size)?,
                    DataType::Int16 => dict_from_scalar::<Int16Type>(v, size)?,
                    DataType::Int32 => dict_from_scalar::<Int32Type>(v, size)?,
                    DataType::Int64 => dict_from_scalar::<Int64Type>(v, size)?,
                    DataType::UInt8 => dict_from_scalar::<UInt8Type>(v, size)?,
                    DataType::UInt16 => dict_from_scalar::<UInt16Type>(v, size)?,
                    DataType::UInt32 => dict_from_scalar::<UInt32Type>(v, size)?,
                    DataType::UInt64 => dict_from_scalar::<UInt64Type>(v, size)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                }
            }
            ScalarValue::Null => new_null_array(&DataType::Null, size),
        })
    }

    fn get_decimal_value_from_array(
        array: &dyn Array,
        index: usize,
        precision: u8,
        scale: i8,
    ) -> Result<ScalarValue> {
        match array.data_type() {
            DataType::Decimal128(_, _) => {
                let array = as_decimal128_array(array)?;
                if array.is_null(index) {
                    Ok(ScalarValue::Decimal128(None, precision, scale))
                } else {
                    let value = array.value(index);
                    Ok(ScalarValue::Decimal128(Some(value), precision, scale))
                }
            }
            DataType::Decimal256(_, _) => {
                let array = as_decimal256_array(array)?;
                if array.is_null(index) {
                    Ok(ScalarValue::Decimal256(None, precision, scale))
                } else {
                    let value = array.value(index);
                    Ok(ScalarValue::Decimal256(Some(value), precision, scale))
                }
            }
            _ => _internal_err!("Unsupported decimal type"),
        }
    }

    /// Retrieve ScalarValue for each row in `array`
    ///
    /// Example
    /// ```
    /// use datafusion_common::ScalarValue;
    /// use arrow::array::ListArray;
    /// use arrow::datatypes::{DataType, Int32Type};
    ///
    /// let list_arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
    ///    Some(vec![Some(1), Some(2), Some(3)]),
    ///    None,
    ///    Some(vec![Some(4), Some(5)])
    /// ]);
    ///
    /// let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&list_arr).unwrap();
    ///
    /// let expected = vec![
    ///   vec![
    ///     ScalarValue::Int32(Some(1)),
    ///     ScalarValue::Int32(Some(2)),
    ///     ScalarValue::Int32(Some(3)),
    ///   ],
    ///   vec![],
    ///   vec![ScalarValue::Int32(Some(4)), ScalarValue::Int32(Some(5))]
    /// ];
    ///
    /// assert_eq!(scalar_vec, expected);
    /// ```
    pub fn convert_array_to_scalar_vec(array: &dyn Array) -> Result<Vec<Vec<Self>>> {
        let mut scalars = Vec::with_capacity(array.len());

        for index in 0..array.len() {
            let scalar_values = match array.data_type() {
                DataType::List(_) => {
                    let list_array = as_list_array(array);
                    match list_array.is_null(index) {
                        true => Vec::new(),
                        false => {
                            let nested_array = list_array.value(index);
                            ScalarValue::convert_array_to_scalar_vec(&nested_array)?
                                .into_iter()
                                .flatten()
                                .collect()
                        }
                    }
                }
                _ => {
                    let scalar = ScalarValue::try_from_array(array, index)?;
                    vec![scalar]
                }
            };
            scalars.push(scalar_values);
        }
        Ok(scalars)
    }

    // TODO: Support more types after other ScalarValue is wrapped with ArrayRef
    /// Get raw data (inner array) inside ScalarValue
    pub fn raw_data(&self) -> Result<ArrayRef> {
        match self {
            ScalarValue::List(arr) => Ok(arr.to_owned()),
            _ => _internal_err!("ScalarValue is not a list"),
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        // handle NULL value
        if !array.is_valid(index) {
            return array.data_type().try_into();
        }

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Decimal128(precision, scale) => {
                ScalarValue::get_decimal_value_from_array(
                    array, index, *precision, *scale,
                )?
            }
            DataType::Decimal256(precision, scale) => {
                ScalarValue::get_decimal_value_from_array(
                    array, index, *precision, *scale,
                )?
            }
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean)?,
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64)?,
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32)?,
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64)?,
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32)?,
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16)?,
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8)?,
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64)?,
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32)?,
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16)?,
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8)?,
            DataType::Binary => typed_cast!(array, index, BinaryArray, Binary)?,
            DataType::LargeBinary => {
                typed_cast!(array, index, LargeBinaryArray, LargeBinary)?
            }
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8)?,
            DataType::LargeUtf8 => {
                typed_cast!(array, index, LargeStringArray, LargeUtf8)?
            }
            DataType::List(_) => {
                let list_array = as_list_array(array);
                let nested_array = list_array.value(index);
                // Produces a single element `ListArray` with the value at `index`.
                let arr = Arc::new(array_into_list_array(nested_array));

                ScalarValue::List(arr)
            }
            // TODO: There is no test for FixedSizeList now, add it later
            DataType::FixedSizeList(_, _) => {
                let list_array = as_fixed_size_list_array(array)?;
                let nested_array = list_array.value(index);
                // Produces a single element `ListArray` with the value at `index`.
                let arr = Arc::new(array_into_list_array(nested_array));

                ScalarValue::List(arr)
            }
            DataType::Date32 => typed_cast!(array, index, Date32Array, Date32)?,
            DataType::Date64 => typed_cast!(array, index, Date64Array, Date64)?,
            DataType::Time32(TimeUnit::Second) => {
                typed_cast!(array, index, Time32SecondArray, Time32Second)?
            }
            DataType::Time32(TimeUnit::Millisecond) => {
                typed_cast!(array, index, Time32MillisecondArray, Time32Millisecond)?
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                typed_cast!(array, index, Time64MicrosecondArray, Time64Microsecond)?
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                typed_cast!(array, index, Time64NanosecondArray, Time64Nanosecond)?
            }
            DataType::Timestamp(TimeUnit::Second, tz_opt) => typed_cast_tz!(
                array,
                index,
                TimestampSecondArray,
                TimestampSecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Millisecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                TimestampMillisecondArray,
                TimestampMillisecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Microsecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                TimestampMicrosecondArray,
                TimestampMicrosecond,
                tz_opt
            )?,
            DataType::Timestamp(TimeUnit::Nanosecond, tz_opt) => typed_cast_tz!(
                array,
                index,
                TimestampNanosecondArray,
                TimestampNanosecond,
                tz_opt
            )?,
            DataType::Dictionary(key_type, _) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index)?,
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index)?,
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index)?,
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index)?,
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index)?,
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index)?,
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index)?,
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index)?,
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
                let array = as_struct_array(array)?;
                let mut field_values: Vec<ScalarValue> = Vec::new();
                for col_index in 0..array.num_columns() {
                    let col_array = array.column(col_index);
                    let col_scalar = ScalarValue::try_from_array(col_array, index)?;
                    field_values.push(col_scalar);
                }
                Self::Struct(Some(field_values), fields.clone())
            }
            DataType::FixedSizeBinary(_) => {
                let array = as_fixed_size_binary_array(array)?;
                let size = match array.data_type() {
                    DataType::FixedSizeBinary(size) => *size,
                    _ => unreachable!(),
                };
                ScalarValue::FixedSizeBinary(
                    size,
                    match array.is_null(index) {
                        true => None,
                        false => Some(array.value(index).into()),
                    },
                )
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                typed_cast!(array, index, IntervalDayTimeArray, IntervalDayTime)?
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                typed_cast!(array, index, IntervalYearMonthArray, IntervalYearMonth)?
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => typed_cast!(
                array,
                index,
                IntervalMonthDayNanoArray,
                IntervalMonthDayNano
            )?,

            DataType::Duration(TimeUnit::Second) => {
                typed_cast!(array, index, DurationSecondArray, DurationSecond)?
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                typed_cast!(array, index, DurationMillisecondArray, DurationMillisecond)?
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                typed_cast!(array, index, DurationMicrosecondArray, DurationMicrosecond)?
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                typed_cast!(array, index, DurationNanosecondArray, DurationNanosecond)?
            }

            other => {
                return _not_impl_err!(
                    "Can't create a scalar from array of type \"{other:?}\""
                );
            }
        })
    }

    /// Try to parse `value` into a ScalarValue of type `target_type`
    pub fn try_from_string(value: String, target_type: &DataType) -> Result<Self> {
        let value = ScalarValue::Utf8(Some(value));
        let cast_options = CastOptions {
            safe: false,
            format_options: Default::default(),
        };
        let cast_arr = cast_with_options(&value.to_array()?, target_type, &cast_options)?;
        ScalarValue::try_from_array(&cast_arr, 0)
    }

    fn eq_array_decimal(
        array: &ArrayRef,
        index: usize,
        value: Option<&i128>,
        precision: u8,
        scale: i8,
    ) -> Result<bool> {
        let array = as_decimal128_array(array)?;
        if array.precision() != precision || array.scale() != scale {
            return Ok(false);
        }
        let is_null = array.is_null(index);
        if let Some(v) = value {
            Ok(!array.is_null(index) && array.value(index) == *v)
        } else {
            Ok(is_null)
        }
    }

    fn eq_array_decimal256(
        array: &ArrayRef,
        index: usize,
        value: Option<&i256>,
        precision: u8,
        scale: i8,
    ) -> Result<bool> {
        let array = as_decimal256_array(array)?;
        if array.precision() != precision || array.scale() != scale {
            return Ok(false);
        }
        let is_null = array.is_null(index);
        if let Some(v) = value {
            Ok(!array.is_null(index) && array.value(index) == *v)
        } else {
            Ok(is_null)
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
    ///
    /// # Errors
    ///
    /// Errors if
    /// - it fails to downcast `array` to the data type of `self`
    /// - `self` is a `Fixedsizelist`
    /// - `self` is a `List`
    /// - `self` is a `Struct`
    ///
    /// # Panics
    ///
    /// Panics if `self` is a dictionary with invalid key type
    #[inline]
    pub fn eq_array(&self, array: &ArrayRef, index: usize) -> Result<bool> {
        Ok(match self {
            ScalarValue::Decimal128(v, precision, scale) => {
                ScalarValue::eq_array_decimal(
                    array,
                    index,
                    v.as_ref(),
                    *precision,
                    *scale,
                )?
            }
            ScalarValue::Decimal256(v, precision, scale) => {
                ScalarValue::eq_array_decimal256(
                    array,
                    index,
                    v.as_ref(),
                    *precision,
                    *scale,
                )?
            }
            ScalarValue::Boolean(val) => {
                eq_array_primitive!(array, index, BooleanArray, val)?
            }
            ScalarValue::Float32(val) => {
                eq_array_primitive!(array, index, Float32Array, val)?
            }
            ScalarValue::Float64(val) => {
                eq_array_primitive!(array, index, Float64Array, val)?
            }
            ScalarValue::Int8(val) => eq_array_primitive!(array, index, Int8Array, val)?,
            ScalarValue::Int16(val) => {
                eq_array_primitive!(array, index, Int16Array, val)?
            }
            ScalarValue::Int32(val) => {
                eq_array_primitive!(array, index, Int32Array, val)?
            }
            ScalarValue::Int64(val) => {
                eq_array_primitive!(array, index, Int64Array, val)?
            }
            ScalarValue::UInt8(val) => {
                eq_array_primitive!(array, index, UInt8Array, val)?
            }
            ScalarValue::UInt16(val) => {
                eq_array_primitive!(array, index, UInt16Array, val)?
            }
            ScalarValue::UInt32(val) => {
                eq_array_primitive!(array, index, UInt32Array, val)?
            }
            ScalarValue::UInt64(val) => {
                eq_array_primitive!(array, index, UInt64Array, val)?
            }
            ScalarValue::Utf8(val) => {
                eq_array_primitive!(array, index, StringArray, val)?
            }
            ScalarValue::LargeUtf8(val) => {
                eq_array_primitive!(array, index, LargeStringArray, val)?
            }
            ScalarValue::Binary(val) => {
                eq_array_primitive!(array, index, BinaryArray, val)?
            }
            ScalarValue::FixedSizeBinary(_, val) => {
                eq_array_primitive!(array, index, FixedSizeBinaryArray, val)?
            }
            ScalarValue::LargeBinary(val) => {
                eq_array_primitive!(array, index, LargeBinaryArray, val)?
            }
            ScalarValue::Fixedsizelist(..) => {
                return _not_impl_err!("FixedSizeList is not supported yet")
            }
            ScalarValue::List(_) => return _not_impl_err!("List is not supported yet"),
            ScalarValue::Date32(val) => {
                eq_array_primitive!(array, index, Date32Array, val)?
            }
            ScalarValue::Date64(val) => {
                eq_array_primitive!(array, index, Date64Array, val)?
            }
            ScalarValue::Time32Second(val) => {
                eq_array_primitive!(array, index, Time32SecondArray, val)?
            }
            ScalarValue::Time32Millisecond(val) => {
                eq_array_primitive!(array, index, Time32MillisecondArray, val)?
            }
            ScalarValue::Time64Microsecond(val) => {
                eq_array_primitive!(array, index, Time64MicrosecondArray, val)?
            }
            ScalarValue::Time64Nanosecond(val) => {
                eq_array_primitive!(array, index, Time64NanosecondArray, val)?
            }
            ScalarValue::TimestampSecond(val, _) => {
                eq_array_primitive!(array, index, TimestampSecondArray, val)?
            }
            ScalarValue::TimestampMillisecond(val, _) => {
                eq_array_primitive!(array, index, TimestampMillisecondArray, val)?
            }
            ScalarValue::TimestampMicrosecond(val, _) => {
                eq_array_primitive!(array, index, TimestampMicrosecondArray, val)?
            }
            ScalarValue::TimestampNanosecond(val, _) => {
                eq_array_primitive!(array, index, TimestampNanosecondArray, val)?
            }
            ScalarValue::IntervalYearMonth(val) => {
                eq_array_primitive!(array, index, IntervalYearMonthArray, val)?
            }
            ScalarValue::IntervalDayTime(val) => {
                eq_array_primitive!(array, index, IntervalDayTimeArray, val)?
            }
            ScalarValue::IntervalMonthDayNano(val) => {
                eq_array_primitive!(array, index, IntervalMonthDayNanoArray, val)?
            }
            ScalarValue::DurationSecond(val) => {
                eq_array_primitive!(array, index, DurationSecondArray, val)?
            }
            ScalarValue::DurationMillisecond(val) => {
                eq_array_primitive!(array, index, DurationMillisecondArray, val)?
            }
            ScalarValue::DurationMicrosecond(val) => {
                eq_array_primitive!(array, index, DurationMicrosecondArray, val)?
            }
            ScalarValue::DurationNanosecond(val) => {
                eq_array_primitive!(array, index, DurationNanosecondArray, val)?
            }
            ScalarValue::Struct(_, _) => {
                return _not_impl_err!("Struct is not supported yet")
            }
            ScalarValue::Dictionary(key_type, v) => {
                let (values_array, values_index) = match key_type.as_ref() {
                    DataType::Int8 => get_dict_value::<Int8Type>(array, index)?,
                    DataType::Int16 => get_dict_value::<Int16Type>(array, index)?,
                    DataType::Int32 => get_dict_value::<Int32Type>(array, index)?,
                    DataType::Int64 => get_dict_value::<Int64Type>(array, index)?,
                    DataType::UInt8 => get_dict_value::<UInt8Type>(array, index)?,
                    DataType::UInt16 => get_dict_value::<UInt16Type>(array, index)?,
                    DataType::UInt32 => get_dict_value::<UInt32Type>(array, index)?,
                    DataType::UInt64 => get_dict_value::<UInt64Type>(array, index)?,
                    _ => unreachable!("Invalid dictionary keys type: {:?}", key_type),
                };
                // was the value in the array non null?
                match values_index {
                    Some(values_index) => v.eq_array(values_array, values_index)?,
                    None => v.is_null(),
                }
            }
            ScalarValue::Null => array.is_null(index),
        })
    }

    /// Estimate size if bytes including `Self`. For values with internal containers such as `String`
    /// includes the allocated size (`capacity`) rather than the current length (`len`)
    pub fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + match self {
                ScalarValue::Null
                | ScalarValue::Boolean(_)
                | ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Decimal256(_, _, _)
                | ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_)
                | ScalarValue::Date32(_)
                | ScalarValue::Date64(_)
                | ScalarValue::Time32Second(_)
                | ScalarValue::Time32Millisecond(_)
                | ScalarValue::Time64Microsecond(_)
                | ScalarValue::Time64Nanosecond(_)
                | ScalarValue::IntervalYearMonth(_)
                | ScalarValue::IntervalDayTime(_)
                | ScalarValue::IntervalMonthDayNano(_)
                | ScalarValue::DurationSecond(_)
                | ScalarValue::DurationMillisecond(_)
                | ScalarValue::DurationMicrosecond(_)
                | ScalarValue::DurationNanosecond(_) => 0,
                ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
                    s.as_ref().map(|s| s.capacity()).unwrap_or_default()
                }
                ScalarValue::TimestampSecond(_, s)
                | ScalarValue::TimestampMillisecond(_, s)
                | ScalarValue::TimestampMicrosecond(_, s)
                | ScalarValue::TimestampNanosecond(_, s) => {
                    s.as_ref().map(|s| s.len()).unwrap_or_default()
                }
                ScalarValue::Binary(b)
                | ScalarValue::FixedSizeBinary(_, b)
                | ScalarValue::LargeBinary(b) => {
                    b.as_ref().map(|b| b.capacity()).unwrap_or_default()
                }
                ScalarValue::Fixedsizelist(vals, field, _) => {
                    vals.as_ref()
                        .map(|vals| Self::size_of_vec(vals) - std::mem::size_of_val(vals))
                        .unwrap_or_default()
                        // `field` is boxed, so it is NOT already included in `self`
                        + field.size()
                }
                ScalarValue::List(arr) => arr.get_array_memory_size(),
                ScalarValue::Struct(vals, fields) => {
                    vals.as_ref()
                        .map(|vals| {
                            vals.iter()
                                .map(|sv| sv.size() - std::mem::size_of_val(sv))
                                .sum::<usize>()
                                + (std::mem::size_of::<ScalarValue>() * vals.capacity())
                        })
                        .unwrap_or_default()
                        // `fields` is boxed, so it is NOT already included in `self`
                        + std::mem::size_of_val(fields)
                        + (std::mem::size_of::<Field>() * fields.len())
                        + fields.iter().map(|field| field.size() - std::mem::size_of_val(field)).sum::<usize>()
                }
                ScalarValue::Dictionary(dt, sv) => {
                    // `dt` and `sv` are boxed, so they are NOT already included in `self`
                    dt.size() + sv.size()
                }
            }
    }

    /// Estimates [size](Self::size) of [`Vec`] in bytes.
    ///
    /// Includes the size of the [`Vec`] container itself.
    pub fn size_of_vec(vec: &Vec<Self>) -> usize {
        std::mem::size_of_val(vec)
            + (std::mem::size_of::<ScalarValue>() * vec.capacity())
            + vec
                .iter()
                .map(|sv| sv.size() - std::mem::size_of_val(sv))
                .sum::<usize>()
    }

    /// Estimates [size](Self::size) of [`HashSet`] in bytes.
    ///
    /// Includes the size of the [`HashSet`] container itself.
    pub fn size_of_hashset<S>(set: &HashSet<Self, S>) -> usize {
        std::mem::size_of_val(set)
            + (std::mem::size_of::<ScalarValue>() * set.capacity())
            + set
                .iter()
                .map(|sv| sv.size() - std::mem::size_of_val(sv))
                .sum::<usize>()
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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(s.into())
    }
}

impl From<Vec<(&str, ScalarValue)>> for ScalarValue {
    fn from(value: Vec<(&str, ScalarValue)>) -> Self {
        let (fields, scalars): (SchemaBuilder, Vec<_>) = value
            .into_iter()
            .map(|(name, scalar)| (Field::new(name, scalar.data_type(), false), scalar))
            .unzip();

        Self::Struct(Some(scalars), fields.finish().fields)
    }
}

macro_rules! impl_try_from {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl TryFrom<ScalarValue> for $NATIVE {
            type Error = DataFusionError;

            fn try_from(value: ScalarValue) -> Result<Self> {
                match value {
                    ScalarValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => _internal_err!(
                        "Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ),
                }
            }
        }
    };
}

impl_try_from!(Int8, i8);
impl_try_from!(Int16, i16);

// special implementation for i32 because of Date32 and Time32
impl TryFrom<ScalarValue> for i32 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int32(Some(inner_value))
            | ScalarValue::Date32(Some(inner_value))
            | ScalarValue::Time32Second(Some(inner_value))
            | ScalarValue::Time32Millisecond(Some(inner_value)) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i64 because of Date64, Time64 and Timestamp
impl TryFrom<ScalarValue> for i64 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Int64(Some(inner_value))
            | ScalarValue::Date64(Some(inner_value))
            | ScalarValue::Time64Microsecond(Some(inner_value))
            | ScalarValue::Time64Nanosecond(Some(inner_value))
            | ScalarValue::TimestampNanosecond(Some(inner_value), _)
            | ScalarValue::TimestampMicrosecond(Some(inner_value), _)
            | ScalarValue::TimestampMillisecond(Some(inner_value), _)
            | ScalarValue::TimestampSecond(Some(inner_value), _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i128 because of Decimal128
impl TryFrom<ScalarValue> for i128 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Decimal128(Some(inner_value), _, _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
        }
    }
}

// special implementation for i256 because of Decimal128
impl TryFrom<ScalarValue> for i256 {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self> {
        match value {
            ScalarValue::Decimal256(Some(inner_value), _, _) => Ok(inner_value),
            _ => _internal_err!(
                "Cannot convert {:?} to {}",
                value,
                std::any::type_name::<Self>()
            ),
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

impl TryFrom<DataType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(datatype: DataType) -> Result<Self> {
        (&datatype).try_into()
    }
}

impl TryFrom<&DataType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
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
            DataType::Decimal256(precision, scale) => {
                ScalarValue::Decimal256(None, *precision, *scale)
            }
            DataType::Utf8 => ScalarValue::Utf8(None),
            DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            DataType::Binary => ScalarValue::Binary(None),
            DataType::FixedSizeBinary(len) => ScalarValue::FixedSizeBinary(*len, None),
            DataType::LargeBinary => ScalarValue::LargeBinary(None),
            DataType::Date32 => ScalarValue::Date32(None),
            DataType::Date64 => ScalarValue::Date64(None),
            DataType::Time32(TimeUnit::Second) => ScalarValue::Time32Second(None),
            DataType::Time32(TimeUnit::Millisecond) => {
                ScalarValue::Time32Millisecond(None)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                ScalarValue::Time64Microsecond(None)
            }
            DataType::Time64(TimeUnit::Nanosecond) => ScalarValue::Time64Nanosecond(None),
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
            DataType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(None)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(None)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(None)
            }

            DataType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(None),
            DataType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(None)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(None)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(None)
            }

            DataType::Dictionary(index_type, value_type) => ScalarValue::Dictionary(
                index_type.clone(),
                Box::new(value_type.as_ref().try_into()?),
            ),
            // `ScalaValue::List` contains single element `ListArray`.
            DataType::List(field) => ScalarValue::List(new_null_array(
                &DataType::List(Arc::new(Field::new(
                    "item",
                    field.data_type().clone(),
                    true,
                ))),
                1,
            )),
            DataType::Struct(fields) => ScalarValue::Struct(None, fields.clone()),
            DataType::Null => ScalarValue::Null,
            _ => {
                return _not_impl_err!(
                    "Can't create a scalar from data_type \"{data_type:?}\""
                );
            }
        })
    }
}

impl TryFrom<LogicalType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this datatype
    fn try_from(datatype: LogicalType) -> Result<Self> {
        (&datatype).try_into()
    }
}

impl TryFrom<&LogicalType> for ScalarValue {
    type Error = DataFusionError;

    /// Create a Null instance of ScalarValue for this LogicalType
    fn try_from(data_type: &LogicalType) -> Result<Self> {
        Ok(match data_type {
            LogicalType::Boolean => ScalarValue::Boolean(None),
            LogicalType::Float64 => ScalarValue::Float64(None),
            LogicalType::Float32 => ScalarValue::Float32(None),
            LogicalType::Int8 => ScalarValue::Int8(None),
            LogicalType::Int16 => ScalarValue::Int16(None),
            LogicalType::Int32 => ScalarValue::Int32(None),
            LogicalType::Int64 => ScalarValue::Int64(None),
            LogicalType::UInt8 => ScalarValue::UInt8(None),
            LogicalType::UInt16 => ScalarValue::UInt16(None),
            LogicalType::UInt32 => ScalarValue::UInt32(None),
            LogicalType::UInt64 => ScalarValue::UInt64(None),
            LogicalType::Decimal128(precision, scale) => {
                ScalarValue::Decimal128(None, *precision, *scale)
            }
            LogicalType::Decimal256(precision, scale) => {
                ScalarValue::Decimal256(None, *precision, *scale)
            }
            LogicalType::Utf8 => ScalarValue::Utf8(None),
            LogicalType::LargeUtf8 => ScalarValue::LargeUtf8(None),
            LogicalType::Binary => ScalarValue::Binary(None),
            LogicalType::FixedSizeBinary(len) => ScalarValue::FixedSizeBinary(*len, None),
            LogicalType::LargeBinary => ScalarValue::LargeBinary(None),
            LogicalType::Date32 => ScalarValue::Date32(None),
            LogicalType::Date64 => ScalarValue::Date64(None),
            LogicalType::Time32(TimeUnit::Second) => ScalarValue::Time32Second(None),
            LogicalType::Time32(TimeUnit::Millisecond) => {
                ScalarValue::Time32Millisecond(None)
            }
            LogicalType::Time64(TimeUnit::Microsecond) => {
                ScalarValue::Time64Microsecond(None)
            }
            LogicalType::Time64(TimeUnit::Nanosecond) => {
                ScalarValue::Time64Nanosecond(None)
            }
            LogicalType::Timestamp(TimeUnit::Second, tz_opt) => {
                ScalarValue::TimestampSecond(None, tz_opt.clone())
            }
            LogicalType::Timestamp(TimeUnit::Millisecond, tz_opt) => {
                ScalarValue::TimestampMillisecond(None, tz_opt.clone())
            }
            LogicalType::Timestamp(TimeUnit::Microsecond, tz_opt) => {
                ScalarValue::TimestampMicrosecond(None, tz_opt.clone())
            }
            LogicalType::Timestamp(TimeUnit::Nanosecond, tz_opt) => {
                ScalarValue::TimestampNanosecond(None, tz_opt.clone())
            }
            LogicalType::Interval(IntervalUnit::YearMonth) => {
                ScalarValue::IntervalYearMonth(None)
            }
            LogicalType::Interval(IntervalUnit::DayTime) => {
                ScalarValue::IntervalDayTime(None)
            }
            LogicalType::Interval(IntervalUnit::MonthDayNano) => {
                ScalarValue::IntervalMonthDayNano(None)
            }

            LogicalType::Duration(TimeUnit::Second) => ScalarValue::DurationSecond(None),
            LogicalType::Duration(TimeUnit::Millisecond) => {
                ScalarValue::DurationMillisecond(None)
            }
            LogicalType::Duration(TimeUnit::Microsecond) => {
                ScalarValue::DurationMicrosecond(None)
            }
            LogicalType::Duration(TimeUnit::Nanosecond) => {
                ScalarValue::DurationNanosecond(None)
            }

            LogicalType::List(_) => ScalarValue::List(new_null_array(&DataType::Null, 0)),

            LogicalType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|e| {
                        Arc::new(Field::new(
                            e.name(),
                            e.data_type().physical_type(),
                            false,
                        ))
                    })
                    .collect::<Vec<_>>();

                ScalarValue::Struct(None, fields.into())
            }
            LogicalType::Null => ScalarValue::Null,
            _ => {
                return _not_impl_err!(
                    "Can't create a scalar from logical_type \"{data_type:?}\""
                );
            }
        })
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{e}"),
            None => write!($F, "NULL"),
        }
    }};
}

// Implement Display trait for ScalarValue
//
// # Panics
//
// Panics if there is an error when creating a visual representation of columns via `arrow::util::pretty`
impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(v, p, s) => {
                write!(f, "{v:?},{p:?},{s:?}")?;
            }
            ScalarValue::Decimal256(v, p, s) => {
                write!(f, "{v:?},{p:?},{s:?}")?;
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
            ScalarValue::Binary(e)
            | ScalarValue::FixedSizeBinary(_, e)
            | ScalarValue::LargeBinary(e) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::Fixedsizelist(e, ..) => match e {
                Some(l) => write!(
                    f,
                    "{}",
                    l.iter()
                        .map(|v| format!("{v}"))
                        .collect::<Vec<_>>()
                        .join(",")
                )?,
                None => write!(f, "NULL")?,
            },
            ScalarValue::List(arr) => write!(
                f,
                "{}",
                arrow::util::pretty::pretty_format_columns("col", &[arr.to_owned()])
                    .unwrap()
            )?,
            ScalarValue::Date32(e) => format_option!(f, e)?,
            ScalarValue::Date64(e) => format_option!(f, e)?,
            ScalarValue::Time32Second(e) => format_option!(f, e)?,
            ScalarValue::Time32Millisecond(e) => format_option!(f, e)?,
            ScalarValue::Time64Microsecond(e) => format_option!(f, e)?,
            ScalarValue::Time64Nanosecond(e) => format_option!(f, e)?,
            ScalarValue::IntervalDayTime(e) => format_option!(f, e)?,
            ScalarValue::IntervalYearMonth(e) => format_option!(f, e)?,
            ScalarValue::IntervalMonthDayNano(e) => format_option!(f, e)?,
            ScalarValue::DurationSecond(e) => format_option!(f, e)?,
            ScalarValue::DurationMillisecond(e) => format_option!(f, e)?,
            ScalarValue::DurationMicrosecond(e) => format_option!(f, e)?,
            ScalarValue::DurationNanosecond(e) => format_option!(f, e)?,
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
            ScalarValue::Dictionary(_k, v) => write!(f, "{v}")?,
            ScalarValue::Null => write!(f, "NULL")?,
        };
        Ok(())
    }
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Decimal128(_, _, _) => write!(f, "Decimal128({self})"),
            ScalarValue::Decimal256(_, _, _) => write!(f, "Decimal256({self})"),
            ScalarValue::Boolean(_) => write!(f, "Boolean({self})"),
            ScalarValue::Float32(_) => write!(f, "Float32({self})"),
            ScalarValue::Float64(_) => write!(f, "Float64({self})"),
            ScalarValue::Int8(_) => write!(f, "Int8({self})"),
            ScalarValue::Int16(_) => write!(f, "Int16({self})"),
            ScalarValue::Int32(_) => write!(f, "Int32({self})"),
            ScalarValue::Int64(_) => write!(f, "Int64({self})"),
            ScalarValue::UInt8(_) => write!(f, "UInt8({self})"),
            ScalarValue::UInt16(_) => write!(f, "UInt16({self})"),
            ScalarValue::UInt32(_) => write!(f, "UInt32({self})"),
            ScalarValue::UInt64(_) => write!(f, "UInt64({self})"),
            ScalarValue::TimestampSecond(_, tz_opt) => {
                write!(f, "TimestampSecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampMillisecond(_, tz_opt) => {
                write!(f, "TimestampMillisecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampMicrosecond(_, tz_opt) => {
                write!(f, "TimestampMicrosecond({self}, {tz_opt:?})")
            }
            ScalarValue::TimestampNanosecond(_, tz_opt) => {
                write!(f, "TimestampNanosecond({self}, {tz_opt:?})")
            }
            ScalarValue::Utf8(None) => write!(f, "Utf8({self})"),
            ScalarValue::Utf8(Some(_)) => write!(f, "Utf8(\"{self}\")"),
            ScalarValue::LargeUtf8(None) => write!(f, "LargeUtf8({self})"),
            ScalarValue::LargeUtf8(Some(_)) => write!(f, "LargeUtf8(\"{self}\")"),
            ScalarValue::Binary(None) => write!(f, "Binary({self})"),
            ScalarValue::Binary(Some(_)) => write!(f, "Binary(\"{self}\")"),
            ScalarValue::FixedSizeBinary(size, None) => {
                write!(f, "FixedSizeBinary({size}, {self})")
            }
            ScalarValue::FixedSizeBinary(size, Some(_)) => {
                write!(f, "FixedSizeBinary({size}, \"{self}\")")
            }
            ScalarValue::LargeBinary(None) => write!(f, "LargeBinary({self})"),
            ScalarValue::LargeBinary(Some(_)) => write!(f, "LargeBinary(\"{self}\")"),
            ScalarValue::Fixedsizelist(..) => write!(f, "FixedSizeList([{self}])"),
            ScalarValue::List(arr) => write!(f, "List([{arr:?}])"),
            ScalarValue::Date32(_) => write!(f, "Date32(\"{self}\")"),
            ScalarValue::Date64(_) => write!(f, "Date64(\"{self}\")"),
            ScalarValue::Time32Second(_) => write!(f, "Time32Second(\"{self}\")"),
            ScalarValue::Time32Millisecond(_) => {
                write!(f, "Time32Millisecond(\"{self}\")")
            }
            ScalarValue::Time64Microsecond(_) => {
                write!(f, "Time64Microsecond(\"{self}\")")
            }
            ScalarValue::Time64Nanosecond(_) => {
                write!(f, "Time64Nanosecond(\"{self}\")")
            }
            ScalarValue::IntervalDayTime(_) => {
                write!(f, "IntervalDayTime(\"{self}\")")
            }
            ScalarValue::IntervalYearMonth(_) => {
                write!(f, "IntervalYearMonth(\"{self}\")")
            }
            ScalarValue::IntervalMonthDayNano(_) => {
                write!(f, "IntervalMonthDayNano(\"{self}\")")
            }
            ScalarValue::DurationSecond(_) => write!(f, "DurationSecond(\"{self}\")"),
            ScalarValue::DurationMillisecond(_) => {
                write!(f, "DurationMillisecond(\"{self}\")")
            }
            ScalarValue::DurationMicrosecond(_) => {
                write!(f, "DurationMicrosecond(\"{self}\")")
            }
            ScalarValue::DurationNanosecond(_) => {
                write!(f, "DurationNanosecond(\"{self}\")")
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
            ScalarValue::Dictionary(k, v) => write!(f, "Dictionary({k:?}, {v:?})"),
            ScalarValue::Null => write!(f, "NULL"),
        }
    }
}

/// Trait used to map a NativeType to a ScalarValue
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
    use std::cmp::Ordering;
    use std::sync::Arc;

    use arrow::compute::kernels;
    use arrow::compute::{concat, is_null};
    use arrow::datatypes::ArrowPrimitiveType;
    use arrow::util::pretty::pretty_format_columns;
    use arrow_array::ArrowNumericType;
    use chrono::NaiveDate;
    use rand::Rng;

    use crate::cast::{as_string_array, as_uint32_array, as_uint64_array};

    use super::*;

    #[test]
    fn test_to_array_of_size_for_list() {
        let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            None,
            Some(2),
        ])]);

        let sv = ScalarValue::List(Arc::new(arr));
        let actual_arr = sv
            .to_array_of_size(2)
            .expect("Failed to convert to array of size");
        let actual_list_arr = as_list_array(&actual_arr);

        let arr = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(2)]),
            Some(vec![Some(1), None, Some(2)]),
        ]);

        assert_eq!(&arr, actual_list_arr);
    }

    #[test]
    fn test_list_to_array_string() {
        let scalars = vec![
            ScalarValue::Utf8(Some(String::from("rust"))),
            ScalarValue::Utf8(Some(String::from("arrow"))),
            ScalarValue::Utf8(Some(String::from("data-fusion"))),
        ];

        let array = ScalarValue::new_list(scalars.as_slice(), &DataType::Utf8);

        let expected = array_into_list_array(Arc::new(StringArray::from(vec![
            "rust",
            "arrow",
            "data-fusion",
        ])));
        let result = as_list_array(&array);
        assert_eq!(result, &expected);
    }

    #[test]
    fn iter_to_array_primitive_test() {
        let scalars = vec![
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            )),
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(4),
                    Some(5),
                ])]),
            )),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let list_array = as_list_array(&array);
        let expected = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(list_array, &expected);
    }

    #[test]
    fn iter_to_array_string_test() {
        let arr1 =
            array_into_list_array(Arc::new(StringArray::from(vec!["foo", "bar", "baz"])));
        let arr2 =
            array_into_list_array(Arc::new(StringArray::from(vec!["rust", "world"])));

        let scalars = vec![
            ScalarValue::List(Arc::new(arr1)),
            ScalarValue::List(Arc::new(arr2)),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let result = as_list_array(&array);

        // build expected array
        let string_builder = StringBuilder::with_capacity(5, 25);
        let mut list_of_string_builder = ListBuilder::new(string_builder);

        list_of_string_builder.values().append_value("foo");
        list_of_string_builder.values().append_value("bar");
        list_of_string_builder.values().append_value("baz");
        list_of_string_builder.append(true);

        list_of_string_builder.values().append_value("rust");
        list_of_string_builder.values().append_value("world");
        list_of_string_builder.append(true);
        let expected = list_of_string_builder.finish();

        assert_eq!(result, &expected);
    }

    #[test]
    fn scalar_add_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            (float_value.add(&float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        assert_eq!(
            (float_value.add(float_value_2))?,
            ScalarValue::Float64(Some(246.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_test() -> Result<()> {
        let float_value = ScalarValue::Float64(Some(123.));
        let float_value_2 = ScalarValue::Float64(Some(123.));
        assert_eq!(
            float_value.sub(&float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        assert_eq!(
            float_value.sub(float_value_2)?,
            ScalarValue::Float64(Some(0.))
        );
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_test() -> Result<()> {
        let int_value = ScalarValue::Int32(Some(42));
        let int_value_2 = ScalarValue::Int32(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int32(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int32(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int32_overflow_test() {
        let int_value = ScalarValue::Int32(Some(i32::MAX));
        let int_value_2 = ScalarValue::Int32(Some(i32::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(
            err,
            "Arrow error: Compute error: Overflow happened on: 2147483647 - -2147483648"
        )
    }

    #[test]
    fn scalar_sub_trait_int64_test() -> Result<()> {
        let int_value = ScalarValue::Int64(Some(42));
        let int_value_2 = ScalarValue::Int64(Some(100));
        assert_eq!(int_value.sub(&int_value_2)?, ScalarValue::Int64(Some(-58)));
        assert_eq!(int_value_2.sub(int_value)?, ScalarValue::Int64(Some(58)));
        Ok(())
    }

    #[test]
    fn scalar_sub_trait_int64_overflow_test() {
        let int_value = ScalarValue::Int64(Some(i64::MAX));
        let int_value_2 = ScalarValue::Int64(Some(i64::MIN));
        let err = int_value
            .sub_checked(&int_value_2)
            .unwrap_err()
            .strip_backtrace();
        assert_eq!(err, "Arrow error: Compute error: Overflow happened on: 9223372036854775807 - -9223372036854775808")
    }

    #[test]
    fn scalar_add_overflow_test() -> Result<()> {
        check_scalar_add_overflow::<Int8Type>(
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(i8::MAX)),
        );
        check_scalar_add_overflow::<UInt8Type>(
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(u8::MAX)),
        );
        check_scalar_add_overflow::<Int16Type>(
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(i16::MAX)),
        );
        check_scalar_add_overflow::<UInt16Type>(
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(u16::MAX)),
        );
        check_scalar_add_overflow::<Int32Type>(
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(i32::MAX)),
        );
        check_scalar_add_overflow::<UInt32Type>(
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(u32::MAX)),
        );
        check_scalar_add_overflow::<Int64Type>(
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(i64::MAX)),
        );
        check_scalar_add_overflow::<UInt64Type>(
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(u64::MAX)),
        );

        Ok(())
    }

    // Verifies that ScalarValue has the same behavior with compute kernal when it overflows.
    fn check_scalar_add_overflow<T>(left: ScalarValue, right: ScalarValue)
    where
        T: ArrowNumericType,
    {
        let scalar_result = left.add_checked(&right);

        let left_array = left.to_array().expect("Failed to convert to array");
        let right_array = right.to_array().expect("Failed to convert to array");
        let arrow_left_array = left_array.as_primitive::<T>();
        let arrow_right_array = right_array.as_primitive::<T>();
        let arrow_result = kernels::numeric::add(arrow_left_array, arrow_right_array);

        assert_eq!(scalar_result.is_ok(), arrow_result.is_ok());
    }

    #[test]
    fn test_interval_add_timestamp() -> Result<()> {
        let interval = ScalarValue::IntervalMonthDayNano(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalYearMonth(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);

        let interval = ScalarValue::IntervalDayTime(Some(123));
        let timestamp = ScalarValue::TimestampNanosecond(Some(123), None);
        let result = interval.add(&timestamp)?;
        let expect = timestamp.add(&interval)?;
        assert_eq!(result, expect);
        Ok(())
    }

    #[test]
    fn scalar_decimal_test() -> Result<()> {
        let decimal_value = ScalarValue::Decimal128(Some(123), 10, 1);
        assert_eq!(DataType::Decimal128(10, 1), decimal_value.data_type());
        let try_into_value: i128 = decimal_value.clone().try_into().unwrap();
        assert_eq!(123_i128, try_into_value);
        assert!(!decimal_value.is_null());
        let neg_decimal_value = decimal_value.arithmetic_negate()?;
        match neg_decimal_value {
            ScalarValue::Decimal128(v, _, _) => {
                assert_eq!(-123, v.unwrap());
            }
            _ => {
                unreachable!();
            }
        }

        // decimal scalar to array
        let array = decimal_value
            .to_array()
            .expect("Failed to convert to array");
        let array = as_decimal128_array(&array)?;
        assert_eq!(1, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array.value(0));

        // decimal scalar to array with size
        let array = decimal_value
            .to_array_of_size(10)
            .expect("Failed to convert to array of size");
        let array_decimal = as_decimal128_array(&array)?;
        assert_eq!(10, array.len());
        assert_eq!(DataType::Decimal128(10, 1), array.data_type().clone());
        assert_eq!(123i128, array_decimal.value(0));
        assert_eq!(123i128, array_decimal.value(9));
        // test eq array
        assert!(decimal_value
            .eq_array(&array, 1)
            .expect("Failed to compare arrays"));
        assert!(decimal_value
            .eq_array(&array, 5)
            .expect("Failed to compare arrays"));
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
        let array = ScalarValue::iter_to_array(decimal_vec).unwrap();
        assert_eq!(3, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
        ];
        let array = ScalarValue::iter_to_array(decimal_vec).unwrap();
        assert_eq!(4, array.len());
        assert_eq!(DataType::Decimal128(10, 2), array.data_type().clone());

        assert!(ScalarValue::try_new_decimal128(1, 10, 2)
            .unwrap()
            .eq_array(&array, 0)
            .expect("Failed to compare arrays"));
        assert!(ScalarValue::try_new_decimal128(2, 10, 2)
            .unwrap()
            .eq_array(&array, 1)
            .expect("Failed to compare arrays"));
        assert!(ScalarValue::try_new_decimal128(3, 10, 2)
            .unwrap()
            .eq_array(&array, 2)
            .expect("Failed to compare arrays"));
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 3).unwrap()
        );

        Ok(())
    }

    #[test]
    fn test_list_partial_cmp() {
        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(30),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Greater));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
                    Some(10),
                    Some(2),
                    Some(30),
                ])]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));

        let a =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(10), Some(2), Some(3)]),
                    None,
                    Some(vec![Some(10), Some(2), Some(3)]),
                ]),
            ));
        let b =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                    Some(vec![Some(10), Some(2), Some(3)]),
                    None,
                    Some(vec![Some(10), Some(2), Some(3)]),
                ]),
            ));
        assert_eq!(a.partial_cmp(&b), Some(Ordering::Equal));
    }

    #[test]
    fn scalar_value_to_array_u64() -> Result<()> {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint64_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_value_to_array_u32() -> Result<()> {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array().expect("Failed to convert to array");
        let array = as_uint32_array(&array)?;
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
        Ok(())
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref = ScalarValue::new_list(&[], &DataType::UInt64);
        let list_array = as_list_array(&list_array_ref);

        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() -> Result<()> {
        let values = vec![
            ScalarValue::UInt64(Some(100)),
            ScalarValue::UInt64(None),
            ScalarValue::UInt64(Some(101)),
        ];
        let list_array_ref = ScalarValue::new_list(&values, &DataType::UInt64);
        let list_array = as_list_array(&list_array_ref);
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = as_uint64_array(&prim_array_ref)?;
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
        Ok(())
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
    // despite clippy claiming they are useless, the code doesn't compile otherwise.
    #[allow(clippy::useless_vec)]
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

        let result = ScalarValue::iter_to_array(scalars).unwrap_err();
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

        let scalars = [
            make_val(Some("Foo".into())),
            make_val(None),
            make_val(Some("Bar".into())),
        ];

        let array = ScalarValue::iter_to_array(scalars).unwrap();
        let array = as_dictionary_array::<Int32Type>(&array).unwrap();
        let values_array = as_string_array(array.values()).unwrap();

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
        let scalars = [Boolean(Some(true)), Int32(Some(5))];

        let result = ScalarValue::iter_to_array(scalars).unwrap_err();
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
    fn scalar_try_from_array_list_array_null() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);

        let non_null_list_scalar = ScalarValue::try_from_array(&list, 0).unwrap();
        let null_list_scalar = ScalarValue::try_from_array(&list, 1).unwrap();

        let data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));

        assert_eq!(non_null_list_scalar.data_type(), data_type.clone());
        assert_eq!(null_list_scalar.data_type(), data_type);
    }

    #[test]
    fn scalar_try_from_list() {
        let data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let data_type = &data_type;
        let scalar: ScalarValue = data_type.try_into().unwrap();

        let expected = ScalarValue::List(new_null_array(
            &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            1,
        ));

        assert_eq!(expected, scalar)
    }

    #[test]
    fn scalar_try_from_list_of_list() {
        let data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )));
        let data_type = &data_type;
        let scalar: ScalarValue = data_type.try_into().unwrap();

        let expected = ScalarValue::List(new_null_array(
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ))),
            1,
        ));

        assert_eq!(expected, scalar)
    }

    #[test]
    fn scalar_try_from_not_equal_list_nested_list() {
        let list_data_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let data_type = &list_data_type;
        let list_scalar: ScalarValue = data_type.try_into().unwrap();

        let nested_list_data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )));
        let data_type = &nested_list_data_type;
        let nested_list_scalar: ScalarValue = data_type.try_into().unwrap();

        assert_ne!(list_scalar, nested_list_scalar);
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

    // this test fails on aarch, so don't run it there
    #[cfg(not(target_arch = "aarch64"))]
    #[test]
    fn size_of_scalar() {
        // Since ScalarValues are used in a non trivial number of places,
        // making it larger means significant more memory consumption
        // per distinct value.
        //
        // The alignment requirements differ across architectures and
        // thus the size of the enum appears to as as well

        assert_eq!(std::mem::size_of::<ScalarValue>(), 48);
    }

    #[test]
    fn memory_size() {
        let sv = ScalarValue::Binary(Some(Vec::with_capacity(10)));
        assert_eq!(sv.size(), std::mem::size_of::<ScalarValue>() + 10,);
        let sv_size = sv.size();

        let mut v = Vec::with_capacity(10);
        // do NOT clone `sv` here because this may shrink the vector capacity
        v.push(sv);
        assert_eq!(v.capacity(), 10);
        assert_eq!(
            ScalarValue::size_of_vec(&v),
            std::mem::size_of::<Vec<ScalarValue>>()
                + (9 * std::mem::size_of::<ScalarValue>())
                + sv_size,
        );

        let mut s = HashSet::with_capacity(0);
        // do NOT clone `sv` here because this may shrink the vector capacity
        s.insert(v.pop().unwrap());
        // hashsets may easily grow during insert, so capacity is dynamic
        let s_capacity = s.capacity();
        assert_eq!(
            ScalarValue::size_of_hashset(&s),
            std::mem::size_of::<HashSet<ScalarValue>>()
                + ((s_capacity - 1) * std::mem::size_of::<ScalarValue>())
                + sv_size,
        );
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

        let bool_vals = [Some(true), None, Some(false)];
        let f32_vals = [Some(-1.0), None, Some(1.0)];
        let f64_vals = make_typed_vec!(f32_vals, f64);

        let i8_vals = [Some(-1), None, Some(1)];
        let i16_vals = make_typed_vec!(i8_vals, i16);
        let i32_vals = make_typed_vec!(i8_vals, i32);
        let i64_vals = make_typed_vec!(i8_vals, i64);

        let u8_vals = [Some(0), None, Some(1)];
        let u16_vals = make_typed_vec!(u8_vals, u16);
        let u32_vals = make_typed_vec!(u8_vals, u32);
        let u64_vals = make_typed_vec!(u8_vals, u64);

        let str_vals = [Some("foo"), None, Some("bar")];

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
            make_test_case!(i32_vals, Time32SecondArray, Time32Second),
            make_test_case!(i32_vals, Time32MillisecondArray, Time32Millisecond),
            make_test_case!(i64_vals, Time64MicrosecondArray, Time64Microsecond),
            make_test_case!(i64_vals, Time64NanosecondArray, Time64Nanosecond),
            make_test_case!(i64_vals, TimestampSecondArray, TimestampSecond, None),
            make_test_case!(
                i64_vals,
                TimestampSecondArray,
                TimestampSecond,
                Some("UTC".into())
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
                Some("UTC".into())
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
                Some("UTC".into())
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
                Some("UTC".into())
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
            println!("Input scalars: {scalars:#?}");
            assert_eq!(array.len(), scalars.len());

            for (index, scalar) in scalars.into_iter().enumerate() {
                assert!(
                    scalar
                        .eq_array(&array, index)
                        .expect("Failed to compare arrays"),
                    "Expected {scalar:?} to be equal to {array:?} at index {index}"
                );

                // test that all other elements are *not* equal
                for other_index in 0..array.len() {
                    if index != other_index {
                        assert!(
                            !scalar.eq_array(&array, other_index).expect("Failed to compare arrays"),
                            "Expected {scalar:?} to be NOT equal to {array:?} at index {other_index}"
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
        let field_a = Arc::new(Field::new("A", DataType::Int32, false));
        let field_b = Arc::new(Field::new("B", DataType::Boolean, false));
        let field_c = Arc::new(Field::new("C", DataType::Utf8, false));

        let field_e = Arc::new(Field::new("e", DataType::Int16, false));
        let field_f = Arc::new(Field::new("f", DataType::Int64, false));
        let field_d = Arc::new(Field::new(
            "D",
            DataType::Struct(vec![field_e.clone(), field_f.clone()].into()),
            false,
        ));

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
            vec![
                field_a.clone(),
                field_b.clone(),
                field_c.clone(),
                field_d.clone(),
            ]
            .into(),
        );

        // Check Display
        assert_eq!(
            format!("{scalar}"),
            String::from("{A:23,B:false,C:Hello,D:{e:2,f:3}}")
        );

        // Check Debug
        assert_eq!(
            format!("{scalar:?}"),
            String::from(
                r#"Struct({A:Int32(23),B:Boolean(false),C:Utf8("Hello"),D:Struct({e:Int16(2),f:Int64(3)})})"#
            )
        );

        // Convert to length-2 array
        let array = scalar
            .to_array_of_size(2)
            .expect("Failed to convert to array of size");

        let expected = Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from(vec![23, 23])) as ArrayRef,
            ),
            (
                field_b.clone(),
                Arc::new(BooleanArray::from(vec![false, false])) as ArrayRef,
            ),
            (
                field_c.clone(),
                Arc::new(StringArray::from(vec!["Hello", "Hello"])) as ArrayRef,
            ),
            (
                field_d.clone(),
                Arc::new(StructArray::from(vec![
                    (
                        field_e.clone(),
                        Arc::new(Int16Array::from(vec![2, 2])) as ArrayRef,
                    ),
                    (
                        field_f.clone(),
                        Arc::new(Int64Array::from(vec![3, 3])) as ArrayRef,
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
        assert_eq!(format!("{none_scalar:?}"), String::from("Struct(NULL)"));

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
                Arc::new(Int32Array::from(vec![23, 7, -1000])) as ArrayRef,
            ),
            (
                field_b,
                Arc::new(BooleanArray::from(vec![false, true, true])) as ArrayRef,
            ),
            (
                field_c,
                Arc::new(StringArray::from(vec!["Hello", "World", "!!!!!"])) as ArrayRef,
            ),
            (
                field_d,
                Arc::new(StructArray::from(vec![
                    (
                        field_e,
                        Arc::new(Int16Array::from(vec![2, 4, 6])) as ArrayRef,
                    ),
                    (
                        field_f,
                        Arc::new(Int64Array::from(vec![3, 5, 7])) as ArrayRef,
                    ),
                ])) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);
    }

    #[test]
    fn test_lists_in_struct() {
        let field_a = Arc::new(Field::new("A", DataType::Utf8, false));
        let field_primitive_list = Arc::new(Field::new(
            "primitive_list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            false,
        ));

        // Define primitive list scalars
        let l0 =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                ])]),
            ));
        let l1 =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(4),
                    Some(5),
                ])]),
            ));
        let l2 = ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<
            Int32Type,
            _,
            _,
        >(vec![Some(vec![Some(6)])])));

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
        let array = as_struct_array(&array).unwrap();
        let expected = StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(StringArray::from(vec!["First", "Second", "Third"])) as ArrayRef,
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

        let nl0_array = ScalarValue::iter_to_array(vec![s0.clone(), s1.clone()]).unwrap();
        let nl0 = ScalarValue::List(Arc::new(array_into_list_array(nl0_array)));

        let nl1_array = ScalarValue::iter_to_array(vec![s2.clone()]).unwrap();
        let nl1 = ScalarValue::List(Arc::new(array_into_list_array(nl1_array)));

        let nl2_array = ScalarValue::iter_to_array(vec![s1.clone()]).unwrap();
        let nl2 = ScalarValue::List(Arc::new(array_into_list_array(nl2_array)));

        // iter_to_array for list-of-struct
        let array = ScalarValue::iter_to_array(vec![nl0, nl1, nl2]).unwrap();
        let array = as_list_array(&array);

        // Construct expected array with array builders
        let field_a_builder = StringBuilder::with_capacity(4, 1024);
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
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(4),
            Some(5),
        ])]);
        let l1 = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            OffsetBuffer::<i32>::from_lengths([1, 1]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(6)])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(7),
            Some(8),
        ])]);
        let l2 = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            OffsetBuffer::<i32>::from_lengths([1, 1]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(9)])]);
        let l3 = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            OffsetBuffer::<i32>::from_lengths([1]),
            arrow::compute::concat(&[&a1]).unwrap(),
            None,
        );

        let array = ScalarValue::iter_to_array(vec![
            ScalarValue::List(Arc::new(l1)),
            ScalarValue::List(Arc::new(l2)),
            ScalarValue::List(Arc::new(l3)),
        ])
        .unwrap();
        let array = as_list_array(&array);

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
            Some("UTC".into()),
        );

        assert_eq!(
            scalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let array = scalar.to_array().expect("Failed to convert to array");
        assert_eq!(array.len(), 1);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );

        let newscalar = ScalarValue::try_from_array(&array, 0).unwrap();
        assert_eq!(
            newscalar.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
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
        let scalar_array = scalar.to_array().expect("Failed to convert to array");
        // cast the actual value
        let cast_array = kernels::cast::cast(&scalar_array, &desired_type).unwrap();

        // turn it back to a scalar
        let cast_scalar = ScalarValue::try_from_array(&cast_array, 0).unwrap();
        assert_eq!(cast_scalar.data_type(), desired_type);

        // Some time later the "cast" scalar is turned back into an array:
        let array = cast_scalar
            .to_array_of_size(10)
            .expect("Failed to convert to array of size");

        // The datatype should be "Dictionary" but is actually Utf8!!!
        assert_eq!(array.data_type(), &desired_type)
    }

    #[test]
    fn test_scalar_negative() -> Result<()> {
        // positive test
        let value = ScalarValue::Int32(Some(12));
        assert_eq!(ScalarValue::Int32(Some(-12)), value.arithmetic_negate()?);
        let value = ScalarValue::Int32(None);
        assert_eq!(ScalarValue::Int32(None), value.arithmetic_negate()?);

        // negative test
        let value = ScalarValue::UInt8(Some(12));
        assert!(value.arithmetic_negate().is_err());
        let value = ScalarValue::Boolean(None);
        assert!(value.arithmetic_negate().is_err());
        Ok(())
    }

    macro_rules! expect_operation_error {
        ($TEST_NAME:ident, $FUNCTION:ident, $EXPECTED_ERROR:expr) => {
            #[test]
            fn $TEST_NAME() {
                let lhs = ScalarValue::UInt64(Some(12));
                let rhs = ScalarValue::Int32(Some(-3));
                match lhs.$FUNCTION(&rhs) {
                    Ok(_result) => {
                        panic!(
                            "Expected binary operation error between lhs: '{:?}', rhs: {:?}",
                            lhs, rhs
                        );
                    }
                    Err(e) => {
                        let error_message = e.to_string();
                        assert!(
                            error_message.contains($EXPECTED_ERROR),
                            "Expected error '{}' not found in actual error '{}'",
                            $EXPECTED_ERROR,
                            error_message
                        );
                    }
                }
            }
        };
    }

    expect_operation_error!(
        expect_add_error,
        add,
        "Invalid arithmetic operation: UInt64 + Int32"
    );
    expect_operation_error!(
        expect_sub_error,
        sub,
        "Invalid arithmetic operation: UInt64 - Int32"
    );

    macro_rules! decimal_op_test_cases {
    ($OPERATION:ident, [$([$L_VALUE:expr, $L_PRECISION:expr, $L_SCALE:expr, $R_VALUE:expr, $R_PRECISION:expr, $R_SCALE:expr, $O_VALUE:expr, $O_PRECISION:expr, $O_SCALE:expr]),+]) => {
            $(

                let left = ScalarValue::Decimal128($L_VALUE, $L_PRECISION, $L_SCALE);
                let right = ScalarValue::Decimal128($R_VALUE, $R_PRECISION, $R_SCALE);
                let result = left.$OPERATION(&right).unwrap();
                assert_eq!(ScalarValue::Decimal128($O_VALUE, $O_PRECISION, $O_SCALE), result);

            )+
        };
    }

    #[test]
    fn decimal_operations() {
        decimal_op_test_cases!(
            add,
            [
                [Some(123), 10, 2, Some(124), 10, 2, Some(123 + 124), 11, 2],
                // test sum decimal with diff scale
                [
                    Some(123),
                    10,
                    3,
                    Some(124),
                    10,
                    2,
                    Some(123 + 124 * 10_i128.pow(1)),
                    12,
                    3
                ],
                // diff precision and scale for decimal data type
                [
                    Some(123),
                    10,
                    2,
                    Some(124),
                    11,
                    3,
                    Some(123 * 10_i128.pow(3 - 2) + 124),
                    12,
                    3
                ]
            ]
        );
    }

    #[test]
    fn decimal_operations_with_nulls() {
        decimal_op_test_cases!(
            add,
            [
                // Case: (None, Some, 0)
                [None, 10, 2, Some(123), 10, 2, None, 11, 2],
                // Case: (Some, None, 0)
                [Some(123), 10, 2, None, 10, 2, None, 11, 2],
                // Case: (Some, None, _) + Side=False
                [Some(123), 8, 2, None, 10, 3, None, 11, 3],
                // Case: (None, Some, _) + Side=False
                [None, 8, 2, Some(123), 10, 3, None, 11, 3],
                // Case: (Some, None, _) + Side=True
                [Some(123), 8, 4, None, 10, 3, None, 12, 4],
                // Case: (None, Some, _) + Side=True
                [None, 10, 3, Some(123), 8, 4, None, 12, 4]
            ]
        );
    }

    #[test]
    fn test_scalar_distance() {
        let cases = [
            // scalar (lhs), scalar (rhs), expected distance
            // ---------------------------------------------
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(Some(2)), 1),
            (ScalarValue::Int8(Some(2)), ScalarValue::Int8(Some(1)), 1),
            (
                ScalarValue::Int16(Some(-5)),
                ScalarValue::Int16(Some(5)),
                10,
            ),
            (
                ScalarValue::Int16(Some(5)),
                ScalarValue::Int16(Some(-5)),
                10,
            ),
            (ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(0)), 0),
            (
                ScalarValue::Int32(Some(-5)),
                ScalarValue::Int32(Some(-10)),
                5,
            ),
            (
                ScalarValue::Int64(Some(-10)),
                ScalarValue::Int64(Some(-5)),
                5,
            ),
            (ScalarValue::UInt8(Some(1)), ScalarValue::UInt8(Some(2)), 1),
            (ScalarValue::UInt8(Some(0)), ScalarValue::UInt8(Some(0)), 0),
            (
                ScalarValue::UInt16(Some(5)),
                ScalarValue::UInt16(Some(10)),
                5,
            ),
            (
                ScalarValue::UInt32(Some(10)),
                ScalarValue::UInt32(Some(5)),
                5,
            ),
            (
                ScalarValue::UInt64(Some(5)),
                ScalarValue::UInt64(Some(10)),
                5,
            ),
            (
                ScalarValue::Float32(Some(1.0)),
                ScalarValue::Float32(Some(2.0)),
                1,
            ),
            (
                ScalarValue::Float32(Some(2.0)),
                ScalarValue::Float32(Some(1.0)),
                1,
            ),
            (
                ScalarValue::Float64(Some(0.0)),
                ScalarValue::Float64(Some(0.0)),
                0,
            ),
            (
                ScalarValue::Float64(Some(-5.0)),
                ScalarValue::Float64(Some(-10.0)),
                5,
            ),
            (
                ScalarValue::Float64(Some(-10.0)),
                ScalarValue::Float64(Some(-5.0)),
                5,
            ),
            // Floats are currently special cased to f64/f32 and the result is rounded
            // rather than ceiled/floored. In the future we might want to take a mode
            // which specified the rounding behavior.
            (
                ScalarValue::Float32(Some(1.2)),
                ScalarValue::Float32(Some(1.3)),
                0,
            ),
            (
                ScalarValue::Float32(Some(1.1)),
                ScalarValue::Float32(Some(1.9)),
                1,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.2)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.7)),
                4,
            ),
            (
                ScalarValue::Float64(Some(-5.3)),
                ScalarValue::Float64(Some(-9.9)),
                5,
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let distance = lhs.distance(rhs).unwrap();
            assert_eq!(distance, *expected);
        }
    }

    #[test]
    fn test_scalar_distance_invalid() {
        let cases = [
            // scalar (lhs), scalar (rhs)
            // --------------------------
            // Same type but with nulls
            (ScalarValue::Int8(None), ScalarValue::Int8(None)),
            (ScalarValue::Int8(None), ScalarValue::Int8(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int8(None)),
            // Different type
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Float32(Some(1.0))),
            (
                ScalarValue::Float64(Some(1.1)),
                ScalarValue::Float32(Some(2.2)),
            ),
            (
                ScalarValue::UInt64(Some(777)),
                ScalarValue::Int32(Some(111)),
            ),
            // Different types with nulls
            (ScalarValue::Int8(None), ScalarValue::Int16(Some(1))),
            (ScalarValue::Int8(Some(1)), ScalarValue::Int16(None)),
            // Unsupported types
            (
                ScalarValue::Utf8(Some("foo".to_string())),
                ScalarValue::Utf8(Some("bar".to_string())),
            ),
            (
                ScalarValue::Boolean(Some(true)),
                ScalarValue::Boolean(Some(false)),
            ),
            (ScalarValue::Date32(Some(0)), ScalarValue::Date32(Some(1))),
            (ScalarValue::Date64(Some(0)), ScalarValue::Date64(Some(1))),
            (
                ScalarValue::Decimal128(Some(123), 5, 5),
                ScalarValue::Decimal128(Some(120), 5, 5),
            ),
        ];
        for (lhs, rhs) in cases {
            let distance = lhs.distance(&rhs);
            assert!(distance.is_none());
        }
    }

    #[test]
    fn test_scalar_interval_negate() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(-1, -12),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(-1, -999),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(-12, -15, -123_456),
            ),
        ];
        for (expr, expected) in cases.iter() {
            let result = expr.arithmetic_negate().unwrap();
            assert_eq!(*expected, result, "-expr:{expr:?}");
        }
    }

    #[test]
    fn test_scalar_interval_add() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(2, 24),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(2, 1998),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(24, 30, 246_912),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.add(rhs).unwrap();
            let result_commute = rhs.add(lhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} + rhs:{rhs:?}");
            assert_eq!(*expected, result_commute, "lhs:{rhs:?} + rhs:{lhs:?}");
        }
    }

    #[test]
    fn test_scalar_interval_sub() {
        let cases = [
            (
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(1, 12),
                ScalarValue::new_interval_ym(0, 0),
            ),
            (
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(1, 999),
                ScalarValue::new_interval_dt(0, 0),
            ),
            (
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(12, 15, 123_456),
                ScalarValue::new_interval_mdn(0, 0, 0),
            ),
        ];
        for (lhs, rhs, expected) in cases.iter() {
            let result = lhs.sub(rhs).unwrap();
            assert_eq!(*expected, result, "lhs:{lhs:?} - rhs:{rhs:?}");
        }
    }

    #[test]
    fn timestamp_op_random_tests() {
        // timestamp1 + (or -) interval = timestamp2
        // timestamp2 - timestamp1 (or timestamp1 - timestamp2) = interval ?
        let sample_size = 1000;
        let timestamps1 = get_random_timestamps(sample_size);
        let intervals = get_random_intervals(sample_size);
        // ts(sec) + interval(ns) = ts(sec); however,
        // ts(sec) - ts(sec) cannot be = interval(ns). Therefore,
        // timestamps are more precise than intervals in tests.
        for (idx, ts1) in timestamps1.iter().enumerate() {
            if idx % 2 == 0 {
                let timestamp2 = ts1.add(intervals[idx].clone()).unwrap();
                let back = timestamp2.sub(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            } else {
                let timestamp2 = ts1.sub(intervals[idx].clone()).unwrap();
                let back = timestamp2.add(intervals[idx].clone()).unwrap();
                assert_eq!(ts1, &back);
            };
        }
    }

    #[test]
    fn test_struct_nulls() {
        let fields_b = Fields::from(vec![
            Field::new("ba", DataType::UInt64, true),
            Field::new("bb", DataType::UInt64, true),
        ]);
        let fields = Fields::from(vec![
            Field::new("a", DataType::UInt64, true),
            Field::new("b", DataType::Struct(fields_b.clone()), true),
        ]);
        let scalars = vec![
            ScalarValue::Struct(None, fields.clone()),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(None),
                    ScalarValue::Struct(None, fields_b.clone()),
                ]),
                fields.clone(),
            ),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(None),
                    ScalarValue::Struct(
                        Some(vec![ScalarValue::UInt64(None), ScalarValue::UInt64(None)]),
                        fields_b.clone(),
                    ),
                ]),
                fields.clone(),
            ),
            ScalarValue::Struct(
                Some(vec![
                    ScalarValue::UInt64(Some(1)),
                    ScalarValue::Struct(
                        Some(vec![
                            ScalarValue::UInt64(Some(2)),
                            ScalarValue::UInt64(Some(3)),
                        ]),
                        fields_b,
                    ),
                ]),
                fields,
            ),
        ];

        let check_array = |array| {
            let is_null = is_null(&array).unwrap();
            assert_eq!(is_null, BooleanArray::from(vec![true, false, false, false]));

            let formatted = pretty_format_columns("col", &[array]).unwrap().to_string();
            let formatted = formatted.split('\n').collect::<Vec<_>>();
            let expected = vec![
                "+---------------------------+",
                "| col                       |",
                "+---------------------------+",
                "|                           |",
                "| {a: , b: }                |",
                "| {a: , b: {ba: , bb: }}    |",
                "| {a: 1, b: {ba: 2, bb: 3}} |",
                "+---------------------------+",
            ];
            assert_eq!(
                formatted, expected,
                "Actual:\n{formatted:#?}\n\nExpected:\n{expected:#?}"
            );
        };

        // test `ScalarValue::iter_to_array`
        let array = ScalarValue::iter_to_array(scalars.clone()).unwrap();
        check_array(array);

        // test `ScalarValue::to_array` / `ScalarValue::to_array_of_size`
        let arrays = scalars
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()
            .expect("Failed to convert to array");
        let arrays = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        let array = concat(&arrays).unwrap();
        check_array(array);
    }

    #[test]
    fn test_build_timestamp_millisecond_list() {
        let values = vec![ScalarValue::TimestampMillisecond(Some(1), None)];
        let arr = ScalarValue::new_list(
            &values,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        );
        assert_eq!(1, arr.len());
    }

    #[test]
    fn test_newlist_timestamp_zone() {
        let s: &'static str = "UTC";
        let values = vec![ScalarValue::TimestampMillisecond(Some(1), Some(s.into()))];
        let arr = ScalarValue::new_list(
            &values,
            &DataType::Timestamp(TimeUnit::Millisecond, Some(s.into())),
        );
        assert_eq!(1, arr.len());
        assert_eq!(
            arr.data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::Timestamp(TimeUnit::Millisecond, Some(s.into())),
                true
            )))
        );
    }

    fn get_random_timestamps(sample_size: u64) -> Vec<ScalarValue> {
        let vector_size = sample_size;
        let mut timestamp = vec![];
        let mut rng = rand::thread_rng();
        for i in 0..vector_size {
            let year = rng.gen_range(1995..=2050);
            let month = rng.gen_range(1..=12);
            let day = rng.gen_range(1..=28); // to exclude invalid dates
            let hour = rng.gen_range(0..=23);
            let minute = rng.gen_range(0..=59);
            let second = rng.gen_range(0..=59);
            if i % 4 == 0 {
                timestamp.push(ScalarValue::TimestampSecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_opt(hour, minute, second)
                            .unwrap()
                            .timestamp(),
                    ),
                    None,
                ))
            } else if i % 4 == 1 {
                let millisec = rng.gen_range(0..=999);
                timestamp.push(ScalarValue::TimestampMillisecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_milli_opt(hour, minute, second, millisec)
                            .unwrap()
                            .timestamp_millis(),
                    ),
                    None,
                ))
            } else if i % 4 == 2 {
                let microsec = rng.gen_range(0..=999_999);
                timestamp.push(ScalarValue::TimestampMicrosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_micro_opt(hour, minute, second, microsec)
                            .unwrap()
                            .timestamp_micros(),
                    ),
                    None,
                ))
            } else if i % 4 == 3 {
                let nanosec = rng.gen_range(0..=999_999_999);
                timestamp.push(ScalarValue::TimestampNanosecond(
                    Some(
                        NaiveDate::from_ymd_opt(year, month, day)
                            .unwrap()
                            .and_hms_nano_opt(hour, minute, second, nanosec)
                            .unwrap()
                            .timestamp_nanos_opt()
                            .unwrap(),
                    ),
                    None,
                ))
            }
        }
        timestamp
    }

    fn get_random_intervals(sample_size: u64) -> Vec<ScalarValue> {
        const MILLISECS_IN_ONE_DAY: i64 = 86_400_000;
        const NANOSECS_IN_ONE_DAY: i64 = 86_400_000_000_000;

        let vector_size = sample_size;
        let mut intervals = vec![];
        let mut rng = rand::thread_rng();
        const SECS_IN_ONE_DAY: i32 = 86_400;
        const MICROSECS_IN_ONE_DAY: i64 = 86_400_000_000;
        for i in 0..vector_size {
            if i % 4 == 0 {
                let days = rng.gen_range(0..5000);
                // to not break second precision
                let millis = rng.gen_range(0..SECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_dt(days, millis));
            } else if i % 4 == 1 {
                let days = rng.gen_range(0..5000);
                let millisec = rng.gen_range(0..(MILLISECS_IN_ONE_DAY as i32));
                intervals.push(ScalarValue::new_interval_dt(days, millisec));
            } else if i % 4 == 2 {
                let days = rng.gen_range(0..5000);
                // to not break microsec precision
                let nanosec = rng.gen_range(0..MICROSECS_IN_ONE_DAY) * 1000;
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            } else {
                let days = rng.gen_range(0..5000);
                let nanosec = rng.gen_range(0..NANOSECS_IN_ONE_DAY);
                intervals.push(ScalarValue::new_interval_mdn(0, days, nanosec));
            }
        }
        intervals
    }
}
