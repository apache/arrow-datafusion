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

//! [`StatisticsConverter`] to convert statistics in parquet format to arrow [`ArrayRef`].

// TODO: potentially move this to arrow-rs: https://github.com/apache/arrow-rs/issues/4328

use arrow::datatypes::i256;
use arrow::{array::ArrayRef, datatypes::DataType};
use arrow_array::{
    new_null_array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};
use arrow_schema::{Field, FieldRef, Schema, TimeUnit};
use datafusion_common::{internal_datafusion_err, internal_err, plan_err, Result};
use half::f16;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex, RowGroupMetaData};
use parquet::file::page_index::index::{Index, PageIndex};
use parquet::file::statistics::Statistics as ParquetStatistics;
use parquet::schema::types::SchemaDescriptor;
use paste::paste;
use std::sync::Arc;

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be::<16>(b))
}

// Convert the bytes array to i256.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i256(b: &[u8]) -> i256 {
    i256::from_be_bytes(sign_extend_be::<32>(b))
}

// Convert the bytes array to f16
pub(crate) fn from_bytes_to_f16(b: &[u8]) -> Option<f16> {
    match b {
        [low, high] => Some(f16::from_be_bytes([*high, *low])),
        _ => None,
    }
}

// Copy from arrow-rs
// https://github.com/apache/arrow-rs/blob/198af7a3f4aa20f9bd003209d9f04b0f37bb120e/parquet/src/arrow/buffer/bit_util.rs#L54
// Convert the byte slice to fixed length byte array with the length of N.
fn sign_extend_be<const N: usize>(b: &[u8]) -> [u8; N] {
    assert!(b.len() <= N, "Array too large, expected less than {N}");
    let is_negative = (b[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - b.len()).zip(b) {
        *d = *s;
    }
    result
}

/// Define an adapter iterator for extracting statistics from an iterator of
/// `ParquetStatistics`
///
///
/// Handles checking if the statistics are present and valid with the correct type.
///
/// Parameters:
/// * `$iterator_type` is the name of the iterator type (e.g. `MinBooleanStatsIterator`)
/// * `$func` is the function to call to get the value (e.g. `min` or `max`)
/// * `$parquet_statistics_type` is the type of the statistics (e.g. `ParquetStatistics::Boolean`)
/// * `$stat_value_type` is the type of the statistics value (e.g. `bool`)
macro_rules! make_stats_iterator {
    ($iterator_type:ident, $func:ident, $parquet_statistics_type:path, $stat_value_type:ty) => {
        /// Maps an iterator of `ParquetStatistics` into an iterator of
        /// `&$stat_value_type``
        ///
        /// Yielded elements:
        /// * Some(stats) if valid
        /// * None if the statistics are not present, not valid, or not $stat_value_type
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            /// Create a new iterator to extract the statistics
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        /// Implement the Iterator trait for the iterator
        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            type Item = Option<&'a $stat_value_type>;

            /// return the next statistics value
            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                next.map(|x| {
                    x.and_then(|stats| match stats {
                        $parquet_statistics_type(s) if stats.has_min_max_set() => {
                            Some(s.$func())
                        }
                        _ => None,
                    })
                })
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_stats_iterator!(
    MinBooleanStatsIterator,
    min,
    ParquetStatistics::Boolean,
    bool
);
make_stats_iterator!(
    MaxBooleanStatsIterator,
    max,
    ParquetStatistics::Boolean,
    bool
);
make_stats_iterator!(MinInt32StatsIterator, min, ParquetStatistics::Int32, i32);
make_stats_iterator!(MaxInt32StatsIterator, max, ParquetStatistics::Int32, i32);
make_stats_iterator!(MinInt64StatsIterator, min, ParquetStatistics::Int64, i64);
make_stats_iterator!(MaxInt64StatsIterator, max, ParquetStatistics::Int64, i64);
make_stats_iterator!(MinFloatStatsIterator, min, ParquetStatistics::Float, f32);
make_stats_iterator!(MaxFloatStatsIterator, max, ParquetStatistics::Float, f32);
make_stats_iterator!(MinDoubleStatsIterator, min, ParquetStatistics::Double, f64);
make_stats_iterator!(MaxDoubleStatsIterator, max, ParquetStatistics::Double, f64);
make_stats_iterator!(
    MinByteArrayStatsIterator,
    min_bytes,
    ParquetStatistics::ByteArray,
    [u8]
);
make_stats_iterator!(
    MaxByteArrayStatsIterator,
    max_bytes,
    ParquetStatistics::ByteArray,
    [u8]
);
make_stats_iterator!(
    MinFixedLenByteArrayStatsIterator,
    min_bytes,
    ParquetStatistics::FixedLenByteArray,
    [u8]
);
make_stats_iterator!(
    MaxFixedLenByteArrayStatsIterator,
    max_bytes,
    ParquetStatistics::FixedLenByteArray,
    [u8]
);

/// Special iterator adapter for extracting i128 values from from an iterator of
/// `ParquetStatistics`
///
/// Handles checking if the statistics are present and valid with the correct type.
///
/// Depending on the parquet file, the statistics for `Decimal128` can be stored as
/// `Int32`, `Int64` or `ByteArray` or `FixedSizeByteArray` :mindblown:
///
/// This iterator handles all cases, extracting the values
/// and converting it to `stat_value_type`.
///
/// Parameters:
/// * `$iterator_type` is the name of the iterator type (e.g. `MinBooleanStatsIterator`)
/// * `$func` is the function to call to get the value (e.g. `min` or `max`)
/// * `$bytes_func` is the function to call to get the value as bytes (e.g. `min_bytes` or `max_bytes`)
/// * `$stat_value_type` is the type of the statistics value (e.g. `i128`)
/// * `convert_func` is the function to convert the bytes to stats value (e.g. `from_bytes_to_i128`)
macro_rules! make_decimal_stats_iterator {
    ($iterator_type:ident, $func:ident, $bytes_func:ident, $stat_value_type:ident, $convert_func: ident) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            type Item = Option<$stat_value_type>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                next.map(|x| {
                    x.and_then(|stats| {
                        if !stats.has_min_max_set() {
                            return None;
                        }
                        match stats {
                            ParquetStatistics::Int32(s) => {
                                Some($stat_value_type::from(*s.$func()))
                            }
                            ParquetStatistics::Int64(s) => {
                                Some($stat_value_type::from(*s.$func()))
                            }
                            ParquetStatistics::ByteArray(s) => {
                                Some($convert_func(s.$bytes_func()))
                            }
                            ParquetStatistics::FixedLenByteArray(s) => {
                                Some($convert_func(s.$bytes_func()))
                            }
                            _ => None,
                        }
                    })
                })
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_decimal_stats_iterator!(
    MinDecimal128StatsIterator,
    min,
    min_bytes,
    i128,
    from_bytes_to_i128
);
make_decimal_stats_iterator!(
    MaxDecimal128StatsIterator,
    max,
    max_bytes,
    i128,
    from_bytes_to_i128
);
make_decimal_stats_iterator!(
    MinDecimal256StatsIterator,
    min,
    min_bytes,
    i256,
    from_bytes_to_i256
);
make_decimal_stats_iterator!(
    MaxDecimal256StatsIterator,
    max,
    max_bytes,
    i256,
    from_bytes_to_i256
);

/// Special macro to combine the statistics iterators for min and max using the [`mod@paste`] macro.
/// This is used to avoid repeating the same code for min and max statistics extractions
///
/// Parameters:
/// stat_type_prefix: The prefix of the statistics iterator type (e.g. `Min` or `Max`)
/// data_type: The data type of the statistics (e.g. `DataType::Int32`)
/// iterator: The iterator of [`ParquetStatistics`] to extract the statistics from.
macro_rules! get_statistics {
    ($stat_type_prefix: ident, $data_type: ident, $iterator: ident) => {
        paste! {
        match $data_type {
            DataType::Boolean => Ok(Arc::new(BooleanArray::from_iter(
                [<$stat_type_prefix BooleanStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Int8 => Ok(Arc::new(Int8Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| i8::try_from(*x).ok())
                }),
            ))),
            DataType::Int16 => Ok(Arc::new(Int16Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| i16::try_from(*x).ok())
                }),
            ))),
            DataType::Int32 => Ok(Arc::new(Int32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Int64 => Ok(Arc::new(Int64Array::from_iter(
                [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::UInt8 => Ok(Arc::new(UInt8Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| u8::try_from(*x).ok())
                }),
            ))),
            DataType::UInt16 => Ok(Arc::new(UInt16Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| u16::try_from(*x).ok())
                }),
            ))),
            DataType::UInt32 => Ok(Arc::new(UInt32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.map(|x| *x as u32)),
            ))),
            DataType::UInt64 => Ok(Arc::new(UInt64Array::from_iter(
                [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.map(|x| *x as u64)),
            ))),
            DataType::Float16 => Ok(Arc::new(Float16Array::from_iter(
                [<$stat_type_prefix FixedLenByteArrayStatsIterator>]::new($iterator).map(|x| x.and_then(|x| {
                    from_bytes_to_f16(x)
                })),
            ))),
            DataType::Float32 => Ok(Arc::new(Float32Array::from_iter(
                [<$stat_type_prefix FloatStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Float64 => Ok(Arc::new(Float64Array::from_iter(
                [<$stat_type_prefix DoubleStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Date32 => Ok(Arc::new(Date32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Date64 => Ok(Arc::new(Date64Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator)
                    .map(|x| x.map(|x| i64::from(*x) * 24 * 60 * 60 * 1000)),
            ))),
            DataType::Timestamp(unit, timezone) =>{
                let iter = [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied());
                Ok(match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                })
            },
            DataType::Time32(unit) => {
                Ok(match unit {
                    TimeUnit::Second =>  Arc::new(Time32SecondArray::from_iter(
                        [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from_iter(
                        [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    _ => {
                        let len = $iterator.count();
                        // don't know how to extract statistics, so return a null array
                        new_null_array($data_type, len)
                    }
                })
            },
            DataType::Time64(unit) => {
                Ok(match unit {
                    TimeUnit::Microsecond =>  Arc::new(Time64MicrosecondArray::from_iter(
                        [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from_iter(
                        [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    _ => {
                        let len = $iterator.count();
                        // don't know how to extract statistics, so return a null array
                        new_null_array($data_type, len)
                    }
                })
            },
            DataType::Binary => Ok(Arc::new(BinaryArray::from_iter(
                [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator).map(|x| x.map(|x| x.to_vec())),
            ))),
            DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from_iter(
                [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator).map(|x| x.map(|x|x.to_vec())),
            ))),
            DataType::Utf8 => Ok(Arc::new(StringArray::from_iter(
                [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| {
                        let res = std::str::from_utf8(x).map(|s| s.to_string()).ok();
                        if res.is_none() {
                            log::debug!("Utf8 statistics is a non-UTF8 value, ignoring it.");
                        }
                        res
                    })
                }),
            ))),
            DataType::LargeUtf8 => {
                Ok(Arc::new(LargeStringArray::from_iter(
                    [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator).map(|x| {
                        x.and_then(|x| {
                            let res = std::str::from_utf8(x).map(|s| s.to_string()).ok();
                            if res.is_none() {
                                log::debug!("LargeUtf8 statistics is a non-UTF8 value, ignoring it.");
                            }
                            res
                        })
                    }),
                )))
            }
            DataType::FixedSizeBinary(size) => Ok(Arc::new(FixedSizeBinaryArray::from(
                [<$stat_type_prefix FixedLenByteArrayStatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| {
                        if x.len().try_into() == Ok(*size) {
                            Some(x)
                        } else {
                            log::debug!(
                                "FixedSizeBinary({}) statistics is a binary of size {}, ignoring it.",
                                size,
                                x.len(),
                            );
                            None
                        }
                    })
                }).collect::<Vec<_>>(),
            ))),
            DataType::Decimal128(precision, scale) => {
                let arr = Decimal128Array::from_iter(
                    [<$stat_type_prefix Decimal128StatsIterator>]::new($iterator)
                ).with_precision_and_scale(*precision, *scale)?;
                Ok(Arc::new(arr))
            },
            DataType::Decimal256(precision, scale) => {
                let arr = Decimal256Array::from_iter(
                    [<$stat_type_prefix Decimal256StatsIterator>]::new($iterator)
                ).with_precision_and_scale(*precision, *scale)?;
                Ok(Arc::new(arr))
            },
            DataType::Dictionary(_, value_type) => {
                [<$stat_type_prefix:lower _ statistics>](value_type, $iterator)
            }

            DataType::Map(_,_) |
            DataType::Duration(_) |
            DataType::Interval(_) |
            DataType::Null |
            DataType::BinaryView |
            DataType::Utf8View |
            DataType::List(_) |
            DataType::ListView(_) |
            DataType::FixedSizeList(_, _) |
            DataType::LargeList(_) |
            DataType::LargeListView(_) |
            DataType::Struct(_) |
            DataType::Union(_, _) |
            DataType::RunEndEncoded(_, _) => {
                let len = $iterator.count();
                // don't know how to extract statistics, so return a null array
                Ok(new_null_array($data_type, len))
            }
        }}}
}

macro_rules! make_data_page_stats_iterator {
    ($iterator_type: ident, $func: expr, $index_type: path, $stat_value_type: ty) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            type Item = Vec<Option<$stat_value_type>>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                match next {
                    Some((len, index)) => match index {
                        $index_type(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| $func(x))
                                .collect::<Vec<_>>(),
                        ),
                        // No matching `Index` found;
                        // thus no statistics that can be extracted.
                        // We return vec![None; len] to effectively
                        // create an arrow null-array with the length
                        // corresponding to the number of entries in
                        // `ParquetOffsetIndex` per row group per column.
                        _ => Some(vec![None; len]),
                    },
                    _ => None,
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_data_page_stats_iterator!(
    MinBooleanDataPageStatsIterator,
    |x: &PageIndex<bool>| { x.min },
    Index::BOOLEAN,
    bool
);
make_data_page_stats_iterator!(
    MaxBooleanDataPageStatsIterator,
    |x: &PageIndex<bool>| { x.max },
    Index::BOOLEAN,
    bool
);
make_data_page_stats_iterator!(
    MinInt32DataPageStatsIterator,
    |x: &PageIndex<i32>| { x.min },
    Index::INT32,
    i32
);
make_data_page_stats_iterator!(
    MaxInt32DataPageStatsIterator,
    |x: &PageIndex<i32>| { x.max },
    Index::INT32,
    i32
);
make_data_page_stats_iterator!(
    MinInt64DataPageStatsIterator,
    |x: &PageIndex<i64>| { x.min },
    Index::INT64,
    i64
);
make_data_page_stats_iterator!(
    MaxInt64DataPageStatsIterator,
    |x: &PageIndex<i64>| { x.max },
    Index::INT64,
    i64
);
make_data_page_stats_iterator!(
    MinFloat16DataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.min.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);
make_data_page_stats_iterator!(
    MaxFloat16DataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.max.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);
make_data_page_stats_iterator!(
    MinFloat32DataPageStatsIterator,
    |x: &PageIndex<f32>| { x.min },
    Index::FLOAT,
    f32
);
make_data_page_stats_iterator!(
    MaxFloat32DataPageStatsIterator,
    |x: &PageIndex<f32>| { x.max },
    Index::FLOAT,
    f32
);
make_data_page_stats_iterator!(
    MinFloat64DataPageStatsIterator,
    |x: &PageIndex<f64>| { x.min },
    Index::DOUBLE,
    f64
);
make_data_page_stats_iterator!(
    MaxFloat64DataPageStatsIterator,
    |x: &PageIndex<f64>| { x.max },
    Index::DOUBLE,
    f64
);

macro_rules! get_decimal_page_stats_iterator {
    ($iterator_type: ident, $func: ident, $stat_value_type: ident, $convert_func: ident) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            type Item = Vec<Option<$stat_value_type>>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                match next {
                    Some((len, index)) => match index {
                        Index::INT32(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    Some($stat_value_type::from(
                                        x.$func.unwrap_or_default(),
                                    ))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        Index::INT64(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    Some($stat_value_type::from(
                                        x.$func.unwrap_or_default(),
                                    ))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        Index::BYTE_ARRAY(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    Some($convert_func(
                                        x.clone().$func.unwrap_or_default().data(),
                                    ))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        Index::FIXED_LEN_BYTE_ARRAY(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    Some($convert_func(
                                        x.clone().$func.unwrap_or_default().data(),
                                    ))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        _ => Some(vec![None; len]),
                    },
                    _ => None,
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

get_decimal_page_stats_iterator!(
    MinDecimal128DataPageStatsIterator,
    min,
    i128,
    from_bytes_to_i128
);

get_decimal_page_stats_iterator!(
    MaxDecimal128DataPageStatsIterator,
    max,
    i128,
    from_bytes_to_i128
);

get_decimal_page_stats_iterator!(
    MinDecimal256DataPageStatsIterator,
    min,
    i256,
    from_bytes_to_i256
);

get_decimal_page_stats_iterator!(
    MaxDecimal256DataPageStatsIterator,
    max,
    i256,
    from_bytes_to_i256
);
make_data_page_stats_iterator!(
    MinByteArrayDataPageStatsIterator,
    |x: &PageIndex<ByteArray>| { x.min.clone() },
    Index::BYTE_ARRAY,
    ByteArray
);
make_data_page_stats_iterator!(
    MaxByteArrayDataPageStatsIterator,
    |x: &PageIndex<ByteArray>| { x.max.clone() },
    Index::BYTE_ARRAY,
    ByteArray
);

macro_rules! get_data_page_statistics {
    ($stat_type_prefix: ident, $data_type: ident, $iterator: ident) => {
        paste! {
            match $data_type {
                Some(DataType::Boolean) => Ok(Arc::new(
                    BooleanArray::from_iter(
                        [<$stat_type_prefix BooleanDataPageStatsIterator>]::new($iterator)
                            .flatten()
                            // BooleanArray::from_iter required a sized iterator, so collect into Vec first
                            .collect::<Vec<_>>()
                            .into_iter()
                    )
                )),
                Some(DataType::UInt8) => Ok(Arc::new(
                    UInt8Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| u8::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                Some(DataType::UInt16) => Ok(Arc::new(
                    UInt16Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| u16::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                Some(DataType::UInt32) => Ok(Arc::new(
                    UInt32Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| u32::try_from(x).ok())
                                })
                            })
                            .flatten()
                ))),
                Some(DataType::UInt64) => Ok(Arc::new(
                    UInt64Array::from_iter(
                        [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| u64::try_from(x).ok())
                                })
                            })
                            .flatten()
                ))),
                Some(DataType::Int8) => Ok(Arc::new(
                    Int8Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| i8::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                Some(DataType::Int16) => Ok(Arc::new(
                    Int16Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| i16::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                Some(DataType::Int32) => Ok(Arc::new(Int32Array::from_iter([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Int64) => Ok(Arc::new(Int64Array::from_iter([<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Float16) => Ok(Arc::new(
                    Float16Array::from_iter(
                        [<$stat_type_prefix Float16DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().filter_map(|x| {
                                    x.and_then(|x| Some(from_bytes_to_f16(x.data())))
                                })
                            })
                            .flatten()
                    )
                )),
                Some(DataType::Float32) => Ok(Arc::new(Float32Array::from_iter([<$stat_type_prefix Float32DataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Float64) => Ok(Arc::new(Float64Array::from_iter([<$stat_type_prefix Float64DataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Binary) => Ok(Arc::new(BinaryArray::from_iter([<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::LargeBinary) => Ok(Arc::new(LargeBinaryArray::from_iter([<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Utf8) => Ok(Arc::new(StringArray::from(
                    [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).map(|x| {
                        x.into_iter().filter_map(|x| {
                        x.and_then(|x| {
                            let res = std::str::from_utf8(x.data()).map(|s| s.to_string()).ok();
                            if res.is_none() {
                                log::debug!("Utf8 statistics is a non-UTF8 value, ignoring it.");
                            }
                            res
                        })
                    })
                    }).flatten().collect::<Vec<_>>(),
                ))),
                Some(DataType::LargeUtf8) => Ok(Arc::new(LargeStringArray::from(
                    [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).map(|x| {
                        x.into_iter().filter_map(|x| {
                            x.and_then(|x| {
                                let res = std::str::from_utf8(x.data()).map(|s| s.to_string()).ok();
                                if res.is_none() {
                                    log::debug!("LargeUtf8 statistics is a non-UTF8 value, ignoring it.");
                                }
                                res
                            })
                    })
                    }).flatten().collect::<Vec<_>>(),
                ))),
                Some(DataType::Timestamp(unit, timezone)) => {
                    let iter = [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten();
                    Ok(match unit {
                        TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    })
                },
                Some(DataType::Date32) => Ok(Arc::new(Date32Array::from_iter([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten()))),
                Some(DataType::Date64) => Ok(
                    Arc::new(
                        Date64Array::from([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter()
                                .filter_map(|x| {
                                    x.and_then(|x| i64::try_from(x).ok())
                                })
                                .map(|x| x * 24 * 60 * 60 * 1000)
                            }).flatten().collect::<Vec<_>>()
                        )
                    )
                ),
                Some(DataType::Decimal128(precision, scale)) => Ok(Arc::new(
                    Decimal128Array::from_iter([<$stat_type_prefix Decimal128DataPageStatsIterator>]::new($iterator).flatten()).with_precision_and_scale(*precision, *scale)?)),
                Some(DataType::Decimal256(precision, scale)) => Ok(Arc::new(
                    Decimal256Array::from_iter([<$stat_type_prefix Decimal256DataPageStatsIterator>]::new($iterator).flatten()).with_precision_and_scale(*precision, *scale)?)),
                _ => unimplemented!()
            }
        }
    }
}

/// Lookups up the parquet column by name
///
/// Returns the parquet column index and the corresponding arrow field
pub(crate) fn parquet_column<'a>(
    parquet_schema: &SchemaDescriptor,
    arrow_schema: &'a Schema,
    name: &str,
) -> Option<(usize, &'a FieldRef)> {
    let (root_idx, field) = arrow_schema.fields.find(name)?;
    if field.data_type().is_nested() {
        // Nested fields are not supported and require non-trivial logic
        // to correctly walk the parquet schema accounting for the
        // logical type rules - <https://github.com/apache/parquet-format/blob/master/LogicalTypes.md>
        //
        // For example a ListArray could correspond to anything from 1 to 3 levels
        // in the parquet schema
        return None;
    }

    // This could be made more efficient (#TBD)
    let parquet_idx = (0..parquet_schema.columns().len())
        .find(|x| parquet_schema.get_column_root_idx(*x) == root_idx)?;
    Some((parquet_idx, field))
}

/// Extracts the min statistics from an iterator of [`ParquetStatistics`] to an
/// [`ArrayRef`]
///
/// This is an internal helper -- see [`StatisticsConverter`] for public API
fn min_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    get_statistics!(Min, data_type, iterator)
}

/// Extracts the max statistics from an iterator of [`ParquetStatistics`] to an [`ArrayRef`]
///
/// This is an internal helper -- see [`StatisticsConverter`] for public API
fn max_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    get_statistics!(Max, data_type, iterator)
}

/// Extracts the min statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
pub(crate) fn min_page_statistics<'a, I>(
    data_type: Option<&DataType>,
    iterator: I,
) -> Result<ArrayRef>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    get_data_page_statistics!(Min, data_type, iterator)
}

/// Extracts the max statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
pub(crate) fn max_page_statistics<'a, I>(
    data_type: Option<&DataType>,
    iterator: I,
) -> Result<ArrayRef>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    get_data_page_statistics!(Max, data_type, iterator)
}

/// Extracts the null count statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
///
/// The returned Array is an [`UInt64Array`]
pub(crate) fn null_counts_page_statistics<'a, I>(iterator: I) -> Result<UInt64Array>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    let iter = iterator.flat_map(|(len, index)| match index {
        Index::NONE => vec![None; len],
        Index::BOOLEAN(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::INT32(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::INT64(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::FLOAT(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::DOUBLE(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::FIXED_LEN_BYTE_ARRAY(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::BYTE_ARRAY(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    });

    Ok(UInt64Array::from_iter(iter))
}

/// Extracts Parquet statistics as Arrow arrays
///
/// This is used to convert Parquet statistics to Arrow arrays, with proper type
/// conversions. This information can be used for pruning parquet files or row
/// groups based on the statistics embedded in parquet files
///
/// # Schemas
///
/// The schema of the parquet file and the arrow schema are used to convert the
/// underlying statistics value (stored as a parquet value) into the
/// corresponding Arrow  value. For example, Decimals are stored as binary in
/// parquet files.
///
/// The parquet_schema and arrow_schema do not have to be identical (for
/// example, the columns may be in different orders and one or the other schemas
/// may have additional columns). The function [`parquet_column`] is used to
/// match the column in the parquet file to the column in the arrow schema.
#[derive(Debug)]
pub struct StatisticsConverter<'a> {
    /// the index of the matched column in the parquet schema
    parquet_index: Option<usize>,
    /// The field (with data type) of the column in the arrow schema
    arrow_field: &'a Field,
}

impl<'a> StatisticsConverter<'a> {
    /// Returns a [`UInt64Array`] with row counts for each row group
    ///
    /// # Return Value
    ///
    /// The returned array has no nulls, and has one value for each row group.
    /// Each value is the number of rows in the row group.
    ///
    /// # Example
    /// ```no_run
    /// # use arrow::datatypes::Schema;
    /// # use arrow_array::ArrayRef;
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # use datafusion::datasource::physical_plan::parquet::StatisticsConverter;
    /// # fn get_parquet_metadata() -> ParquetMetaData { unimplemented!() }
    /// # fn get_arrow_schema() -> Schema { unimplemented!() }
    /// // Given the metadata for a parquet file and the arrow schema
    /// let metadata: ParquetMetaData = get_parquet_metadata();
    /// let arrow_schema: Schema = get_arrow_schema();
    /// let parquet_schema = metadata.file_metadata().schema_descr();
    /// // create a converter
    /// let converter = StatisticsConverter::try_new("foo", &arrow_schema, parquet_schema)
    ///   .unwrap();
    /// // get the row counts for each row group
    /// let row_counts = converter.row_group_row_counts(metadata
    ///   .row_groups()
    ///   .iter()
    /// );
    /// ```
    pub fn row_group_row_counts<I>(&self, metadatas: I) -> Result<Option<UInt64Array>>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let Some(_) = self.parquet_index else {
            return Ok(None);
        };

        let mut builder = UInt64Array::builder(10);
        for metadata in metadatas.into_iter() {
            let row_count = metadata.num_rows();
            let row_count: u64 = row_count.try_into().map_err(|e| {
                internal_datafusion_err!(
                    "Parquet row count {row_count} too large to convert to u64: {e}"
                )
            })?;
            builder.append_value(row_count);
        }
        Ok(Some(builder.finish()))
    }

    /// Create a new `StatisticsConverter` to extract statistics for a column
    ///
    /// Note if there is no corresponding column in the parquet file, the returned
    /// arrays will be null. This can happen if the column is in the arrow
    /// schema but not in the parquet schema due to schema evolution.
    ///
    /// See example on [`Self::row_group_mins`] for usage
    ///
    /// # Errors
    ///
    /// * If the column is not found in the arrow schema
    pub fn try_new<'b>(
        column_name: &'b str,
        arrow_schema: &'a Schema,
        parquet_schema: &'a SchemaDescriptor,
    ) -> Result<Self> {
        // ensure the requested column is in the arrow schema
        let Some((_idx, arrow_field)) = arrow_schema.column_with_name(column_name) else {
            return plan_err!(
                "Column '{}' not found in schema for statistics conversion",
                column_name
            );
        };

        // find the column in the parquet schema, if not, return a null array
        let parquet_index = match parquet_column(
            parquet_schema,
            arrow_schema,
            column_name,
        ) {
            Some((parquet_idx, matched_field)) => {
                // sanity check that matching field matches the arrow field
                if matched_field.as_ref() != arrow_field {
                    return internal_err!(
                        "Matched column '{:?}' does not match original matched column '{:?}'",
                        matched_field,
                        arrow_field
                    );
                }
                Some(parquet_idx)
            }
            None => None,
        };

        Ok(Self {
            parquet_index,
            arrow_field,
        })
    }

    /// Extract the minimum values from row group statistics in [`RowGroupMetaData`]
    ///
    /// # Return Value
    ///
    /// The returned array contains 1 value for each row group, in the same order as `metadatas`
    ///
    /// Each value is either
    /// * the minimum value for the column
    /// * a null value, if the statistics can not be extracted
    ///
    /// Note that a null value does NOT mean the min value was actually
    /// `null` it means it the requested statistic is unknown
    ///
    /// # Errors
    ///
    /// Reasons for not being able to extract the statistics include:
    /// * the column is not present in the parquet file
    /// * statistics for the column are not present in the row group
    /// * the stored statistic value can not be converted to the requested type
    ///
    /// # Example
    /// ```no_run
    /// # use arrow::datatypes::Schema;
    /// # use arrow_array::ArrayRef;
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # use datafusion::datasource::physical_plan::parquet::StatisticsConverter;
    /// # fn get_parquet_metadata() -> ParquetMetaData { unimplemented!() }
    /// # fn get_arrow_schema() -> Schema { unimplemented!() }
    /// // Given the metadata for a parquet file and the arrow schema
    /// let metadata: ParquetMetaData = get_parquet_metadata();
    /// let arrow_schema: Schema = get_arrow_schema();
    /// let parquet_schema = metadata.file_metadata().schema_descr();
    /// // create a converter
    /// let converter = StatisticsConverter::try_new("foo", &arrow_schema, parquet_schema)
    ///   .unwrap();
    /// // get the minimum value for the column "foo" in the parquet file
    /// let min_values: ArrayRef = converter
    ///   .row_group_mins(metadata.row_groups().iter())
    ///  .unwrap();
    /// ```
    pub fn row_group_mins<I>(&self, metadatas: I) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_index else {
            return Ok(self.make_null_array(data_type, metadatas));
        };

        let iter = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics());
        min_statistics(data_type, iter)
    }

    /// Extract the maximum values from row group statistics in [`RowGroupMetaData`]
    ///
    /// See docs on [`Self::row_group_mins`] for details
    pub fn row_group_maxes<I>(&self, metadatas: I) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_index else {
            return Ok(self.make_null_array(data_type, metadatas));
        };

        let iter = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics());
        max_statistics(data_type, iter)
    }

    /// Extract the null counts from row group statistics in [`RowGroupMetaData`]
    ///
    /// See docs on [`Self::row_group_mins`] for details
    pub fn row_group_null_counts<I>(&self, metadatas: I) -> Result<UInt64Array>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let Some(parquet_index) = self.parquet_index else {
            let num_row_groups = metadatas.into_iter().count();
            return Ok(UInt64Array::from_iter(
                std::iter::repeat(None).take(num_row_groups),
            ));
        };

        let null_counts = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics())
            .map(|s| s.map(|s| s.null_count()));
        Ok(UInt64Array::from_iter(null_counts))
    }

    /// Extract the minimum values from Data Page statistics.
    ///
    /// In Parquet files, in addition to the Column Chunk level statistics
    /// (stored for each column for each row group) there are also
    /// optional statistics stored for each data page, as part of
    /// the [`ParquetColumnIndex`].
    ///
    /// Since a single Column Chunk is stored as one or more pages,
    /// page level statistics can prune at a finer granularity.
    ///
    /// However since they are stored in a separate metadata
    /// structure ([`Index`]) there is different code to extract them as
    /// compared to arrow statistics.
    ///
    /// # Parameters:
    ///
    /// * `column_page_index`: The parquet column page indices, read from
    /// `ParquetMetaData` column_index
    ///
    /// * `column_offset_index`: The parquet column offset indices, read from
    /// `ParquetMetaData` offset_index
    ///
    /// * `row_group_indices`: The indices of the row groups, that are used to
    /// extract the column page index and offset index on a per row group
    /// per column basis.
    ///
    /// # Return Value
    ///
    /// The returned array contains 1 value for each `NativeIndex`
    /// in the underlying `Index`es, in the same order as they appear
    /// in `metadatas`.
    ///
    /// For example, if there are two `Index`es in `metadatas`:
    /// 1. the first having `3` `PageIndex` entries
    /// 2. the second having `2` `PageIndex` entries
    ///
    /// The returned array would have 5 rows.
    ///
    /// Each value is either:
    /// * the minimum value for the page
    /// * a null value, if the statistics can not be extracted
    ///
    /// Note that a null value does NOT mean the min value was actually
    /// `null` it means it the requested statistic is unknown
    ///
    /// # Errors
    ///
    /// Reasons for not being able to extract the statistics include:
    /// * the column is not present in the parquet file
    /// * statistics for the pages are not present in the row group
    /// * the stored statistic value can not be converted to the requested type
    pub fn data_page_mins<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_index else {
            return Ok(self.make_null_array(data_type, row_group_indices));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index].len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });

        min_page_statistics(Some(data_type), iter)
    }

    /// Extract the maximum values from Data Page statistics.
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_maxes<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_index else {
            return Ok(self.make_null_array(data_type, row_group_indices));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index].len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });

        max_page_statistics(Some(data_type), iter)
    }

    /// Extract the null counts from Data Page statistics.
    ///
    /// The returned Array is an [`UInt64Array`]
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_null_counts<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<UInt64Array>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let Some(parquet_index) = self.parquet_index else {
            let num_row_groups = row_group_indices.into_iter().count();
            return Ok(UInt64Array::from_iter(
                std::iter::repeat(None).take(num_row_groups),
            ));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index].len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });
        null_counts_page_statistics(iter)
    }

    /// Returns an [`ArrayRef`] with row counts for each row group.
    ///
    /// This function iterates over the given row group indexes and computes
    /// the row count for each page in the specified column.
    ///
    /// # Parameters:
    ///
    /// * `column_offset_index`: The parquet column offset indices, read from
    /// `ParquetMetaData` offset_index
    ///
    /// * `row_group_metadatas`: The metadata slice of the row groups, read
    /// from `ParquetMetaData` row_groups
    ///
    /// * `row_group_indices`: The indices of the row groups, that are used to
    /// extract the column offset index on a per row group per column basis.
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_row_counts<I>(
        &self,
        column_offset_index: &ParquetOffsetIndex,
        row_group_metadatas: &'a [RowGroupMetaData],
        row_group_indices: I,
    ) -> Result<Option<UInt64Array>>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let Some(parquet_index) = self.parquet_index else {
            // no matching column found in parquet_index;
            // thus we cannot extract page_locations in order to determine
            // the row count on a per DataPage basis.
            return Ok(None);
        };

        let mut row_count_total = Vec::new();
        for rg_idx in row_group_indices {
            let page_locations = &column_offset_index[*rg_idx][parquet_index];

            let row_count_per_page = page_locations.windows(2).map(|loc| {
                Some(loc[1].first_row_index as u64 - loc[0].first_row_index as u64)
            });

            // append the last page row count
            let num_rows_in_row_group = &row_group_metadatas[*rg_idx].num_rows();
            let row_count_per_page = row_count_per_page
                .chain(std::iter::once(Some(
                    *num_rows_in_row_group as u64
                        - page_locations.last().unwrap().first_row_index as u64,
                )))
                .collect::<Vec<_>>();

            row_count_total.extend(row_count_per_page);
        }

        Ok(Some(UInt64Array::from_iter(row_count_total)))
    }

    /// Returns a null array of data_type with one element per row group
    fn make_null_array<I, A>(&self, data_type: &DataType, metadatas: I) -> ArrayRef
    where
        I: IntoIterator<Item = A>,
    {
        // column was in the arrow schema but not in the parquet schema, so return a null array
        let num_row_groups = metadatas.into_iter().count();
        new_null_array(data_type, num_row_groups)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::compute::kernels::cast_utils::Parser;
    use arrow::datatypes::{i256, Date32Type, Date64Type};
    use arrow_array::{
        new_empty_array, new_null_array, Array, BinaryArray, BooleanArray, Date32Array,
        Date64Array, Decimal128Array, Decimal256Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, RecordBatch,
        StringArray, StructArray, TimestampNanosecondArray,
    };
    use arrow_schema::{Field, SchemaRef};
    use bytes::Bytes;
    use datafusion_common::test_util::parquet_test_data;
    use parquet::arrow::arrow_reader::ArrowReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::metadata::{ParquetMetaData, RowGroupMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};
    use std::path::PathBuf;
    use std::sync::Arc;

    // TODO error cases (with parquet statistics that are mismatched in expected type)

    #[test]
    fn roundtrip_empty() {
        let empty_bool_array = new_empty_array(&DataType::Boolean);
        Test {
            input: empty_bool_array.clone(),
            expected_min: empty_bool_array.clone(),
            expected_max: empty_bool_array.clone(),
        }
        .run()
    }

    #[test]
    fn roundtrip_bool() {
        Test {
            input: bool_array([
                // row group 1
                Some(true),
                None,
                Some(true),
                // row group 2
                Some(true),
                Some(false),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: bool_array([Some(true), Some(false), None]),
            expected_max: bool_array([Some(true), Some(true), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_int32() {
        Test {
            input: i32_array([
                // row group 1
                Some(1),
                None,
                Some(3),
                // row group 2
                Some(0),
                Some(5),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: i32_array([Some(1), Some(0), None]),
            expected_max: i32_array([Some(3), Some(5), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_int64() {
        Test {
            input: i64_array([
                // row group 1
                Some(1),
                None,
                Some(3),
                // row group 2
                Some(0),
                Some(5),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: i64_array([Some(1), Some(0), None]),
            expected_max: i64_array(vec![Some(3), Some(5), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_f32() {
        Test {
            input: f32_array([
                // row group 1
                Some(1.0),
                None,
                Some(3.0),
                // row group 2
                Some(-1.0),
                Some(5.0),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: f32_array([Some(1.0), Some(-1.0), None]),
            expected_max: f32_array([Some(3.0), Some(5.0), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_f64() {
        Test {
            input: f64_array([
                // row group 1
                Some(1.0),
                None,
                Some(3.0),
                // row group 2
                Some(-1.0),
                Some(5.0),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: f64_array([Some(1.0), Some(-1.0), None]),
            expected_max: f64_array([Some(3.0), Some(5.0), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_timestamp() {
        Test {
            input: timestamp_seconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                None,
            ),
            expected_min: timestamp_seconds_array([Some(1), Some(5), None], None),
            expected_max: timestamp_seconds_array([Some(3), Some(9), None], None),
        }
        .run();

        Test {
            input: timestamp_milliseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                None,
            ),
            expected_min: timestamp_milliseconds_array([Some(1), Some(5), None], None),
            expected_max: timestamp_milliseconds_array([Some(3), Some(9), None], None),
        }
        .run();

        Test {
            input: timestamp_microseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                None,
            ),
            expected_min: timestamp_microseconds_array([Some(1), Some(5), None], None),
            expected_max: timestamp_microseconds_array([Some(3), Some(9), None], None),
        }
        .run();

        Test {
            input: timestamp_nanoseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                None,
            ),
            expected_min: timestamp_nanoseconds_array([Some(1), Some(5), None], None),
            expected_max: timestamp_nanoseconds_array([Some(3), Some(9), None], None),
        }
        .run()
    }

    #[test]
    fn roundtrip_timestamp_timezoned() {
        Test {
            input: timestamp_seconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                Some("UTC"),
            ),
            expected_min: timestamp_seconds_array([Some(1), Some(5), None], Some("UTC")),
            expected_max: timestamp_seconds_array([Some(3), Some(9), None], Some("UTC")),
        }
        .run();

        Test {
            input: timestamp_milliseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                Some("UTC"),
            ),
            expected_min: timestamp_milliseconds_array(
                [Some(1), Some(5), None],
                Some("UTC"),
            ),
            expected_max: timestamp_milliseconds_array(
                [Some(3), Some(9), None],
                Some("UTC"),
            ),
        }
        .run();

        Test {
            input: timestamp_microseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                Some("UTC"),
            ),
            expected_min: timestamp_microseconds_array(
                [Some(1), Some(5), None],
                Some("UTC"),
            ),
            expected_max: timestamp_microseconds_array(
                [Some(3), Some(9), None],
                Some("UTC"),
            ),
        }
        .run();

        Test {
            input: timestamp_nanoseconds_array(
                [
                    // row group 1
                    Some(1),
                    None,
                    Some(3),
                    // row group 2
                    Some(9),
                    Some(5),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ],
                Some("UTC"),
            ),
            expected_min: timestamp_nanoseconds_array(
                [Some(1), Some(5), None],
                Some("UTC"),
            ),
            expected_max: timestamp_nanoseconds_array(
                [Some(3), Some(9), None],
                Some("UTC"),
            ),
        }
        .run()
    }

    #[test]
    fn roundtrip_decimal() {
        Test {
            input: Arc::new(
                Decimal128Array::from(vec![
                    // row group 1
                    Some(100),
                    None,
                    Some(22000),
                    // row group 2
                    Some(500000),
                    Some(330000),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ])
                .with_precision_and_scale(9, 2)
                .unwrap(),
            ),
            expected_min: Arc::new(
                Decimal128Array::from(vec![Some(100), Some(330000), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
            expected_max: Arc::new(
                Decimal128Array::from(vec![Some(22000), Some(500000), None])
                    .with_precision_and_scale(9, 2)
                    .unwrap(),
            ),
        }
        .run();

        Test {
            input: Arc::new(
                Decimal256Array::from(vec![
                    // row group 1
                    Some(i256::from(100)),
                    None,
                    Some(i256::from(22000)),
                    // row group 2
                    Some(i256::MAX),
                    Some(i256::MIN),
                    None,
                    // row group 3
                    None,
                    None,
                    None,
                ])
                .with_precision_and_scale(76, 76)
                .unwrap(),
            ),
            expected_min: Arc::new(
                Decimal256Array::from(vec![Some(i256::from(100)), Some(i256::MIN), None])
                    .with_precision_and_scale(76, 76)
                    .unwrap(),
            ),
            expected_max: Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from(22000)),
                    Some(i256::MAX),
                    None,
                ])
                .with_precision_and_scale(76, 76)
                .unwrap(),
            ),
        }
        .run()
    }

    #[test]
    fn roundtrip_utf8() {
        Test {
            input: utf8_array([
                // row group 1
                Some("A"),
                None,
                Some("Q"),
                // row group 2
                Some("ZZ"),
                Some("AA"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: utf8_array([Some("A"), Some("AA"), None]),
            expected_max: utf8_array([Some("Q"), Some("ZZ"), None]),
        }
        .run()
    }

    #[test]
    fn roundtrip_struct() {
        let mut test = Test {
            input: struct_array(vec![
                // row group 1
                (Some(true), Some(1)),
                (None, None),
                (Some(true), Some(3)),
                // row group 2
                (Some(true), Some(0)),
                (Some(false), Some(5)),
                (None, None),
                // row group 3
                (None, None),
                (None, None),
                (None, None),
            ]),
            expected_min: struct_array(vec![
                (Some(true), Some(1)),
                (Some(true), Some(0)),
                (None, None),
            ]),

            expected_max: struct_array(vec![
                (Some(true), Some(3)),
                (Some(true), Some(0)),
                (None, None),
            ]),
        };
        // Due to https://github.com/apache/datafusion/issues/8334,
        // statistics for struct arrays are not supported
        test.expected_min =
            new_null_array(test.input.data_type(), test.expected_min.len());
        test.expected_max =
            new_null_array(test.input.data_type(), test.expected_min.len());
        test.run()
    }

    #[test]
    fn roundtrip_binary() {
        Test {
            input: Arc::new(BinaryArray::from_opt_vec(vec![
                // row group 1
                Some(b"A"),
                None,
                Some(b"Q"),
                // row group 2
                Some(b"ZZ"),
                Some(b"AA"),
                None,
                // row group 3
                None,
                None,
                None,
            ])),
            expected_min: Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"A"),
                Some(b"AA"),
                None,
            ])),
            expected_max: Arc::new(BinaryArray::from_opt_vec(vec![
                Some(b"Q"),
                Some(b"ZZ"),
                None,
            ])),
        }
        .run()
    }

    #[test]
    fn roundtrip_date32() {
        Test {
            input: date32_array(vec![
                // row group 1
                Some("2021-01-01"),
                None,
                Some("2021-01-03"),
                // row group 2
                Some("2021-01-01"),
                Some("2021-01-05"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: date32_array(vec![
                Some("2021-01-01"),
                Some("2021-01-01"),
                None,
            ]),
            expected_max: date32_array(vec![
                Some("2021-01-03"),
                Some("2021-01-05"),
                None,
            ]),
        }
        .run()
    }

    #[test]
    fn roundtrip_date64() {
        Test {
            input: date64_array(vec![
                // row group 1
                Some("2021-01-01"),
                None,
                Some("2021-01-03"),
                // row group 2
                Some("2021-01-01"),
                Some("2021-01-05"),
                None,
                // row group 3
                None,
                None,
                None,
            ]),
            expected_min: date64_array(vec![
                Some("2021-01-01"),
                Some("2021-01-01"),
                None,
            ]),
            expected_max: date64_array(vec![
                Some("2021-01-03"),
                Some("2021-01-05"),
                None,
            ]),
        }
        .run()
    }

    #[test]
    fn roundtrip_large_binary_array() {
        let input: Vec<Option<&[u8]>> = vec![
            // row group 1
            Some(b"A"),
            None,
            Some(b"Q"),
            // row group 2
            Some(b"ZZ"),
            Some(b"AA"),
            None,
            // row group 3
            None,
            None,
            None,
        ];

        let expected_min: Vec<Option<&[u8]>> = vec![Some(b"A"), Some(b"AA"), None];
        let expected_max: Vec<Option<&[u8]>> = vec![Some(b"Q"), Some(b"ZZ"), None];

        Test {
            input: large_binary_array(input),
            expected_min: large_binary_array(expected_min),
            expected_max: large_binary_array(expected_max),
        }
        .run();
    }

    #[test]
    fn struct_and_non_struct() {
        // Ensures that statistics for an array that appears *after* a struct
        // array are not wrong
        let struct_col = struct_array(vec![
            // row group 1
            (Some(true), Some(1)),
            (None, None),
            (Some(true), Some(3)),
        ]);
        let int_col = i32_array([Some(100), Some(200), Some(300)]);
        let expected_min = i32_array([Some(100)]);
        let expected_max = i32_array(vec![Some(300)]);

        // use a name that shadows a name in the struct column
        match struct_col.data_type() {
            DataType::Struct(fields) => {
                assert_eq!(fields.get(1).unwrap().name(), "int_col")
            }
            _ => panic!("unexpected data type for struct column"),
        };

        let input_batch = RecordBatch::try_from_iter([
            ("struct_col", struct_col),
            ("int_col", int_col),
        ])
        .unwrap();

        let schema = input_batch.schema();

        let metadata = parquet_metadata(schema.clone(), input_batch);
        let parquet_schema = metadata.file_metadata().schema_descr();

        // read the int_col statistics
        let (idx, _) = parquet_column(parquet_schema, &schema, "int_col").unwrap();
        assert_eq!(idx, 2);

        let row_groups = metadata.row_groups();
        let converter =
            StatisticsConverter::try_new("int_col", &schema, parquet_schema).unwrap();

        let min = converter.row_group_mins(row_groups.iter()).unwrap();
        assert_eq!(
            &min,
            &expected_min,
            "Min. Statistics\n\n{}\n\n",
            DisplayStats(row_groups)
        );

        let max = converter.row_group_maxes(row_groups.iter()).unwrap();
        assert_eq!(
            &max,
            &expected_max,
            "Max. Statistics\n\n{}\n\n",
            DisplayStats(row_groups)
        );
    }

    #[test]
    fn nan_in_stats() {
        // /parquet-testing/data/nan_in_stats.parquet
        // row_groups: 1
        // "x": Double({min: Some(1.0), max: Some(NaN), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})

        TestFile::new("nan_in_stats.parquet")
            .with_column(ExpectedColumn {
                name: "x",
                expected_min: Arc::new(Float64Array::from(vec![Some(1.0)])),
                expected_max: Arc::new(Float64Array::from(vec![Some(f64::NAN)])),
            })
            .run();
    }

    #[test]
    fn alltypes_plain() {
        // /parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet
        // row_groups: 1
        // (has no statistics)
        TestFile::new("alltypes_plain.parquet")
            // No column statistics should be read as NULL, but with the right type
            .with_column(ExpectedColumn {
                name: "id",
                expected_min: i32_array([None]),
                expected_max: i32_array([None]),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: bool_array([None]),
                expected_max: bool_array([None]),
            })
            .run();
    }

    #[test]
    fn alltypes_tiny_pages() {
        // /parquet-testing/data/alltypes_tiny_pages.parquet
        // row_groups: 1
        // "id": Int32({min: Some(0), max: Some(7299), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "bool_col": Boolean({min: Some(false), max: Some(true), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "tinyint_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "smallint_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "int_col": Int32({min: Some(0), max: Some(9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "bigint_col": Int64({min: Some(0), max: Some(90), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "float_col": Float({min: Some(0.0), max: Some(9.9), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "double_col": Double({min: Some(0.0), max: Some(90.89999999999999), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "date_string_col": ByteArray({min: Some(ByteArray { data: "01/01/09" }), max: Some(ByteArray { data: "12/31/10" }), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "string_col": ByteArray({min: Some(ByteArray { data: "0" }), max: Some(ByteArray { data: "9" }), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "timestamp_col": Int96({min: None, max: None, distinct_count: None, null_count: 0, min_max_deprecated: true, min_max_backwards_compatible: true})
        // "year": Int32({min: Some(2009), max: Some(2010), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        // "month": Int32({min: Some(1), max: Some(12), distinct_count: None, null_count: 0, min_max_deprecated: false, min_max_backwards_compatible: false})
        TestFile::new("alltypes_tiny_pages.parquet")
            .with_column(ExpectedColumn {
                name: "id",
                expected_min: i32_array([Some(0)]),
                expected_max: i32_array([Some(7299)]),
            })
            .with_column(ExpectedColumn {
                name: "bool_col",
                expected_min: bool_array([Some(false)]),
                expected_max: bool_array([Some(true)]),
            })
            .with_column(ExpectedColumn {
                name: "tinyint_col",
                expected_min: i8_array([Some(0)]),
                expected_max: i8_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "smallint_col",
                expected_min: i16_array([Some(0)]),
                expected_max: i16_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "int_col",
                expected_min: i32_array([Some(0)]),
                expected_max: i32_array([Some(9)]),
            })
            .with_column(ExpectedColumn {
                name: "bigint_col",
                expected_min: i64_array([Some(0)]),
                expected_max: i64_array([Some(90)]),
            })
            .with_column(ExpectedColumn {
                name: "float_col",
                expected_min: f32_array([Some(0.0)]),
                expected_max: f32_array([Some(9.9)]),
            })
            .with_column(ExpectedColumn {
                name: "double_col",
                expected_min: f64_array([Some(0.0)]),
                expected_max: f64_array([Some(90.89999999999999)]),
            })
            .with_column(ExpectedColumn {
                name: "date_string_col",
                expected_min: utf8_array([Some("01/01/09")]),
                expected_max: utf8_array([Some("12/31/10")]),
            })
            .with_column(ExpectedColumn {
                name: "string_col",
                expected_min: utf8_array([Some("0")]),
                expected_max: utf8_array([Some("9")]),
            })
            // File has no min/max for timestamp_col
            .with_column(ExpectedColumn {
                name: "timestamp_col",
                expected_min: timestamp_nanoseconds_array([None], None),
                expected_max: timestamp_nanoseconds_array([None], None),
            })
            .with_column(ExpectedColumn {
                name: "year",
                expected_min: i32_array([Some(2009)]),
                expected_max: i32_array([Some(2010)]),
            })
            .with_column(ExpectedColumn {
                name: "month",
                expected_min: i32_array([Some(1)]),
                expected_max: i32_array([Some(12)]),
            })
            .run();
    }

    #[test]
    fn fixed_length_decimal_legacy() {
        // /parquet-testing/data/fixed_length_decimal_legacy.parquet
        // row_groups: 1
        // "value": FixedLenByteArray({min: Some(FixedLenByteArray(ByteArray { data: Some(ByteBufferPtr { data: b"\0\0\0\0\0\xc8" }) })), max: Some(FixedLenByteArray(ByteArray { data: "\0\0\0\0\t`" })), distinct_count: None, null_count: 0, min_max_deprecated: true, min_max_backwards_compatible: true})

        TestFile::new("fixed_length_decimal_legacy.parquet")
            .with_column(ExpectedColumn {
                name: "value",
                expected_min: Arc::new(
                    Decimal128Array::from(vec![Some(200)])
                        .with_precision_and_scale(13, 2)
                        .unwrap(),
                ),
                expected_max: Arc::new(
                    Decimal128Array::from(vec![Some(2400)])
                        .with_precision_and_scale(13, 2)
                        .unwrap(),
                ),
            })
            .run();
    }

    const ROWS_PER_ROW_GROUP: usize = 3;

    /// Writes the input batch into a parquet file, with every every three rows as
    /// their own row group, and compares the min/maxes to the expected values
    struct Test {
        input: ArrayRef,
        expected_min: ArrayRef,
        expected_max: ArrayRef,
    }

    impl Test {
        fn run(self) {
            let Self {
                input,
                expected_min,
                expected_max,
            } = self;

            let input_batch = RecordBatch::try_from_iter([("c1", input)]).unwrap();

            let schema = input_batch.schema();

            let metadata = parquet_metadata(schema.clone(), input_batch);
            let parquet_schema = metadata.file_metadata().schema_descr();

            let row_groups = metadata.row_groups();

            for field in schema.fields() {
                if field.data_type().is_nested() {
                    let lookup = parquet_column(parquet_schema, &schema, field.name());
                    assert_eq!(lookup, None);
                    continue;
                }

                let converter =
                    StatisticsConverter::try_new(field.name(), &schema, parquet_schema)
                        .unwrap();

                assert_eq!(converter.arrow_field, field.as_ref());

                let mins = converter.row_group_mins(row_groups.iter()).unwrap();
                assert_eq!(
                    &mins,
                    &expected_min,
                    "Min. Statistics\n\n{}\n\n",
                    DisplayStats(row_groups)
                );

                let maxes = converter.row_group_maxes(row_groups.iter()).unwrap();
                assert_eq!(
                    &maxes,
                    &expected_max,
                    "Max. Statistics\n\n{}\n\n",
                    DisplayStats(row_groups)
                );
            }
        }
    }

    /// Write the specified batches out as parquet and return the metadata
    fn parquet_metadata(schema: SchemaRef, batch: RecordBatch) -> Arc<ParquetMetaData> {
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(ROWS_PER_ROW_GROUP)
            .build();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let reader = ArrowReaderBuilder::try_new(Bytes::from(buffer)).unwrap();
        reader.metadata().clone()
    }

    /// Formats the statistics nicely for display
    struct DisplayStats<'a>(&'a [RowGroupMetaData]);
    impl<'a> std::fmt::Display for DisplayStats<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let row_groups = self.0;
            writeln!(f, "  row_groups: {}", row_groups.len())?;
            for rg in row_groups {
                for col in rg.columns() {
                    if let Some(statistics) = col.statistics() {
                        writeln!(f, "   {}: {:?}", col.column_path(), statistics)?;
                    }
                }
            }
            Ok(())
        }
    }

    struct ExpectedColumn {
        name: &'static str,
        expected_min: ArrayRef,
        expected_max: ArrayRef,
    }

    /// Reads statistics out of the specified, and compares them to the expected values
    struct TestFile {
        file_name: &'static str,
        expected_columns: Vec<ExpectedColumn>,
    }

    impl TestFile {
        fn new(file_name: &'static str) -> Self {
            Self {
                file_name,
                expected_columns: Vec::new(),
            }
        }

        fn with_column(mut self, column: ExpectedColumn) -> Self {
            self.expected_columns.push(column);
            self
        }

        /// Reads the specified parquet file and validates that the expected min/max
        /// values for the specified columns are as expected.
        fn run(self) {
            let path = PathBuf::from(parquet_test_data()).join(self.file_name);
            let file = std::fs::File::open(path).unwrap();
            let reader = ArrowReaderBuilder::try_new(file).unwrap();
            let arrow_schema = reader.schema();
            let metadata = reader.metadata();
            let row_groups = metadata.row_groups();
            let parquet_schema = metadata.file_metadata().schema_descr();

            for expected_column in self.expected_columns {
                let ExpectedColumn {
                    name,
                    expected_min,
                    expected_max,
                } = expected_column;

                let converter =
                    StatisticsConverter::try_new(name, arrow_schema, parquet_schema)
                        .unwrap();
                let actual_min = converter.row_group_mins(row_groups.iter()).unwrap();
                assert_eq!(&expected_min, &actual_min, "column {name}");

                let actual_max = converter.row_group_maxes(row_groups.iter()).unwrap();
                assert_eq!(&expected_max, &actual_max, "column {name}");
            }
        }
    }

    fn bool_array(input: impl IntoIterator<Item = Option<bool>>) -> ArrayRef {
        let array: BooleanArray = input.into_iter().collect();
        Arc::new(array)
    }

    fn i8_array(input: impl IntoIterator<Item = Option<i8>>) -> ArrayRef {
        let array: Int8Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i16_array(input: impl IntoIterator<Item = Option<i16>>) -> ArrayRef {
        let array: Int16Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i32_array(input: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
        let array: Int32Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn i64_array(input: impl IntoIterator<Item = Option<i64>>) -> ArrayRef {
        let array: Int64Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn f32_array(input: impl IntoIterator<Item = Option<f32>>) -> ArrayRef {
        let array: Float32Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn f64_array(input: impl IntoIterator<Item = Option<f64>>) -> ArrayRef {
        let array: Float64Array = input.into_iter().collect();
        Arc::new(array)
    }

    fn timestamp_seconds_array(
        input: impl IntoIterator<Item = Option<i64>>,
        timzezone: Option<&str>,
    ) -> ArrayRef {
        let array: TimestampSecondArray = input.into_iter().collect();
        match timzezone {
            Some(tz) => Arc::new(array.with_timezone(tz)),
            None => Arc::new(array),
        }
    }

    fn timestamp_milliseconds_array(
        input: impl IntoIterator<Item = Option<i64>>,
        timzezone: Option<&str>,
    ) -> ArrayRef {
        let array: TimestampMillisecondArray = input.into_iter().collect();
        match timzezone {
            Some(tz) => Arc::new(array.with_timezone(tz)),
            None => Arc::new(array),
        }
    }

    fn timestamp_microseconds_array(
        input: impl IntoIterator<Item = Option<i64>>,
        timzezone: Option<&str>,
    ) -> ArrayRef {
        let array: TimestampMicrosecondArray = input.into_iter().collect();
        match timzezone {
            Some(tz) => Arc::new(array.with_timezone(tz)),
            None => Arc::new(array),
        }
    }

    fn timestamp_nanoseconds_array(
        input: impl IntoIterator<Item = Option<i64>>,
        timzezone: Option<&str>,
    ) -> ArrayRef {
        let array: TimestampNanosecondArray = input.into_iter().collect();
        match timzezone {
            Some(tz) => Arc::new(array.with_timezone(tz)),
            None => Arc::new(array),
        }
    }

    fn utf8_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array: StringArray = input
            .into_iter()
            .map(|s| s.map(|s| s.to_string()))
            .collect();
        Arc::new(array)
    }

    // returns a struct array with columns "bool_col" and "int_col" with the specified values
    fn struct_array(input: Vec<(Option<bool>, Option<i32>)>) -> ArrayRef {
        let boolean: BooleanArray = input.iter().map(|(b, _i)| b).collect();
        let int: Int32Array = input.iter().map(|(_b, i)| i).collect();

        let nullable = true;
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("bool_col", DataType::Boolean, nullable)),
                Arc::new(boolean) as ArrayRef,
            ),
            (
                Arc::new(Field::new("int_col", DataType::Int32, nullable)),
                Arc::new(int) as ArrayRef,
            ),
        ]);
        Arc::new(struct_array)
    }

    fn date32_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array = Date32Array::from(
            input
                .into_iter()
                .map(|s| Date32Type::parse(s.unwrap_or_default()))
                .collect::<Vec<_>>(),
        );
        Arc::new(array)
    }

    fn date64_array<'a>(input: impl IntoIterator<Item = Option<&'a str>>) -> ArrayRef {
        let array = Date64Array::from(
            input
                .into_iter()
                .map(|s| Date64Type::parse(s.unwrap_or_default()))
                .collect::<Vec<_>>(),
        );
        Arc::new(array)
    }

    fn large_binary_array<'a>(
        input: impl IntoIterator<Item = Option<&'a [u8]>>,
    ) -> ArrayRef {
        let array =
            LargeBinaryArray::from(input.into_iter().collect::<Vec<Option<&[u8]>>>());

        Arc::new(array)
    }
}
