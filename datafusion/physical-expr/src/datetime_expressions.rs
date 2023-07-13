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

//! DateTime expressions

use arrow::array::Float64Builder;
use arrow::compute::cast;
use arrow::{
    array::TimestampNanosecondArray, compute::kernels::temporal, datatypes::TimeUnit,
    temporal_conversions::timestamp_ns_to_datetime,
};
use arrow::{
    array::{Array, ArrayRef, Float64Array, OffsetSizeTrait, PrimitiveArray},
    compute::kernels::cast_utils::string_to_timestamp_nanos,
    datatypes::{
        ArrowNumericType, ArrowPrimitiveType, ArrowTemporalType, DataType,
        IntervalDayTimeType, IntervalMonthDayNanoType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use arrow_array::{
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray,
};
use chrono::prelude::*;
use chrono::{Duration, Months, NaiveDate};
use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_generic_string_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_common::{ScalarType, ScalarValue};
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
pub(crate) fn unary_string_to_primitive_function<'a, T, O, F>(
    args: &[&'a dyn Array],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str) -> Result<O::Native>,
{
    if args.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name,
        )));
    }

    let array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}

// given an function that maps a `&str` to a arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
fn handle<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i64, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function {name}",
            ))),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {other:?} for function {name}"
            ))),
        },
    }
}

/// Calls string_to_timestamp_nanos and converts the error type
fn string_to_timestamp_nanos_shim(s: &str) -> Result<i64> {
    string_to_timestamp_nanos(s).map_err(|e| e.into())
}

/// to_timestamp SQL function
pub fn to_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampNanosecondType, _, TimestampNanosecondType>(
        args,
        string_to_timestamp_nanos_shim,
        "to_timestamp",
    )
}

/// to_timestamp_millis SQL function
pub fn to_timestamp_millis(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampMillisecondType, _, TimestampMillisecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000_000),
        "to_timestamp_millis",
    )
}

/// to_timestamp_micros SQL function
pub fn to_timestamp_micros(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampMicrosecondType, _, TimestampMicrosecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000),
        "to_timestamp_micros",
    )
}

/// to_timestamp_seconds SQL function
pub fn to_timestamp_seconds(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    handle::<TimestampSecondType, _, TimestampSecondType>(
        args,
        |s| string_to_timestamp_nanos_shim(s).map(|n| n / 1_000_000_000),
        "to_timestamp_seconds",
    )
}

/// Create an implementation of `now()` that always returns the
/// specified timestamp.
///
/// The semantics of `now()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_now(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let now_ts = Some(now_ts.timestamp_nanos());
    move |_arg| {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            now_ts,
            Some("+00:00".into()),
        )))
    }
}

/// Create an implementation of `current_timestamp()` that always returns the
/// specified timestamp.
///
/// The implementation is same with `now()`
pub fn make_current_timestamp(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    make_now(now_ts)
}

/// Create an implementation of `current_date()` that always returns the
/// specified current date.
///
/// The semantics of `current_date()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_date(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let days = Some(
        now_ts.num_days_from_ce()
            - NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .num_days_from_ce(),
    );
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Date32(days)))
}

/// Create an implementation of `current_time()` that always returns the
/// specified current time.
///
/// The semantics of `current_time()` require it to return the same value
/// wherever it appears within a single statement. This value is
/// chosen during planning time.
pub fn make_current_time(
    now_ts: DateTime<Utc>,
) -> impl Fn(&[ColumnarValue]) -> Result<ColumnarValue> {
    let nano = Some(now_ts.timestamp_nanos() % 86400000000000);
    move |_arg| Ok(ColumnarValue::Scalar(ScalarValue::Time64Nanosecond(nano)))
}

fn quarter_month(date: &NaiveDateTime) -> u32 {
    1 + 3 * ((date.month() - 1) / 3)
}

/// Tuncates the single `value`, expressed in nanoseconds since the
/// epoch, for granularities greater than 1 second, in taking into
/// account that some granularities are not uniform durations of time
/// (e.g. months are not always the same lengths, leap seconds, etc)
fn date_trunc_coarse(granularity: &str, value: i64) -> Result<i64> {
    // Use chrono NaiveDateTime to clear the various fields
    // correctly accounting for non uniform granularities
    let value = timestamp_ns_to_datetime(value).ok_or_else(|| {
        DataFusionError::Execution(format!("Timestamp {value} out of range"))
    })?;

    let value = Some(value);

    let value = match granularity {
        "millisecond" => value,
        "microsecond" => value,
        "second" => value.and_then(|d| d.with_nanosecond(0)),
        "minute" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0)),
        "hour" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0)),
        "day" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0)),
        "week" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .map(|d| d - Duration::seconds(60 * 60 * 24 * d.weekday() as i64)),
        "month" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0)),
        "quarter" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month(quarter_month(&d))),
        "year" => value
            .and_then(|d| d.with_nanosecond(0))
            .and_then(|d| d.with_second(0))
            .and_then(|d| d.with_minute(0))
            .and_then(|d| d.with_hour(0))
            .and_then(|d| d.with_day0(0))
            .and_then(|d| d.with_month0(0)),
        unsupported => {
            return Err(DataFusionError::Execution(format!(
                "Unsupported date_trunc granularity: {unsupported}"
            )));
        }
    };
    // `with_x(0)` are infallible because `0` are always a valid
    Ok(value.unwrap().timestamp_nanos())
}

// truncates a single value with the given timeunit to the specified granularity
fn _date_trunc(
    tu: TimeUnit,
    value: &Option<i64>,
    granularity: &str,
) -> Result<Option<i64>, DataFusionError> {
    let scale = match tu {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };

    let Some(value) = value else {
        return Ok(None);
    };

    // convert to nanoseconds
    let nano = date_trunc_coarse(granularity, scale * value)?;

    let result = match tu {
        TimeUnit::Second => match granularity {
            "minute" => Some(nano / 1_000_000_000 / 60 * 60),
            _ => Some(nano / 1_000_000_000),
        },
        TimeUnit::Millisecond => match granularity {
            "minute" => Some(nano / 1_000_000 / 1_000 / 60 * 1_000 * 60),
            "second" => Some(nano / 1_000_000 / 1_000 * 1_000),
            _ => Some(nano / 1_000_000),
        },
        TimeUnit::Microsecond => match granularity {
            "minute" => Some(nano / 1_000 / 1_000_000 / 60 * 60 * 1_000_000),
            "second" => Some(nano / 1_000 / 1_000_000 * 1_000_000),
            "millisecond" => Some(nano / 1_000 / 1_000 * 1_000),
            _ => Some(nano / 1_000),
        },
        _ => match granularity {
            "minute" => Some(nano / 1_000_000_000 / 60 * 1_000_000_000 * 60),
            "second" => Some(nano / 1_000_000_000 * 1_000_000_000),
            "millisecond" => Some(nano / 1_000_000 * 1_000_000),
            "microsecond" => Some(nano / 1_000 * 1_000),
            _ => Some(nano),
        },
    };
    Ok(result)
}

/// date_trunc SQL function
pub fn date_trunc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (granularity, array) = (&args[0], &args[1]);

    let granularity =
        if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = granularity {
            v.to_lowercase()
        } else {
            return Err(DataFusionError::Execution(
                "Granularity of `date_trunc` must be non-null scalar Utf8".to_string(),
            ));
        };

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            let value = _date_trunc(TimeUnit::Nanosecond, v, granularity.as_str())?;
            let value = ScalarValue::TimestampNanosecond(value, tz_opt.clone());
            ColumnarValue::Scalar(value)
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            let value = _date_trunc(TimeUnit::Microsecond, v, granularity.as_str())?;
            let value = ScalarValue::TimestampMicrosecond(value, tz_opt.clone());
            ColumnarValue::Scalar(value)
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            let value = _date_trunc(TimeUnit::Millisecond, v, granularity.as_str())?;
            let value = ScalarValue::TimestampMillisecond(value, tz_opt.clone());
            ColumnarValue::Scalar(value)
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            let value = _date_trunc(TimeUnit::Second, v, granularity.as_str())?;
            let value = ScalarValue::TimestampSecond(value, tz_opt.clone());
            ColumnarValue::Scalar(value)
        }
        ColumnarValue::Array(array) => {
            let array_type = array.data_type();
            match array_type {
                DataType::Timestamp(TimeUnit::Second, _) => {
                    let array = as_timestamp_second_array(array)?;
                    let array = array
                        .iter()
                        .map(|x| _date_trunc(TimeUnit::Second, &x, granularity.as_str()))
                        .collect::<Result<TimestampSecondArray>>()?;
                    ColumnarValue::Array(Arc::new(array))
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let array = as_timestamp_millisecond_array(array)?;
                    let array = array
                        .iter()
                        .map(|x| {
                            _date_trunc(TimeUnit::Millisecond, &x, granularity.as_str())
                        })
                        .collect::<Result<TimestampMillisecondArray>>()?;
                    ColumnarValue::Array(Arc::new(array))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    let array = as_timestamp_microsecond_array(array)?;
                    let array = array
                        .iter()
                        .map(|x| {
                            _date_trunc(TimeUnit::Microsecond, &x, granularity.as_str())
                        })
                        .collect::<Result<TimestampMicrosecondArray>>()?;
                    ColumnarValue::Array(Arc::new(array))
                }
                _ => {
                    let array = as_timestamp_nanosecond_array(array)?;
                    let array = array
                        .iter()
                        .map(|x| {
                            _date_trunc(TimeUnit::Nanosecond, &x, granularity.as_str())
                        })
                        .collect::<Result<TimestampNanosecondArray>>()?;

                    ColumnarValue::Array(Arc::new(array))
                }
            }
        }
        _ => {
            return Err(DataFusionError::Execution(
                "second argument of `date_trunc` must be nanosecond timestamp scalar or array".to_string(),
            ));
        }
    })
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_nanos_interval(stride_nanos: i64, source: i64, origin: i64) -> i64 {
    let time_diff = source - origin;

    // distance from origin to bin
    let time_delta = compute_distance(time_diff, stride_nanos);

    origin + time_delta
}

// distance from origin to bin
fn compute_distance(time_diff: i64, stride: i64) -> i64 {
    let time_delta = time_diff - (time_diff % stride);

    if time_diff < 0 && stride > 1 {
        // The origin is later than the source timestamp, round down to the previous bin
        time_delta - stride
    } else {
        time_delta
    }
}

// return time in nanoseconds that the source timestamp falls into based on the stride and origin
fn date_bin_months_interval(stride_months: i64, source: i64, origin: i64) -> i64 {
    // convert source and origin to DateTime<Utc>
    let source_date = to_utc_date_time(source);
    let origin_date = to_utc_date_time(origin);

    // calculate the number of months between the source and origin
    let month_diff = (source_date.year() - origin_date.year()) * 12
        + source_date.month() as i32
        - origin_date.month() as i32;

    // distance from origin to bin
    let month_delta = compute_distance(month_diff as i64, stride_months);

    let mut bin_time = if month_delta < 0 {
        origin_date - Months::new(month_delta.unsigned_abs() as u32)
    } else {
        origin_date + Months::new(month_delta as u32)
    };

    // If origin is not midnight of first date of the month, the bin_time may be larger than the source
    // In this case, we need to move back to previous bin
    if bin_time > source_date {
        let month_delta = month_delta - stride_months;
        bin_time = if month_delta < 0 {
            origin_date - Months::new(month_delta.unsigned_abs() as u32)
        } else {
            origin_date + Months::new(month_delta as u32)
        };
    }

    bin_time.timestamp_nanos()
}

fn to_utc_date_time(nanos: i64) -> DateTime<Utc> {
    let secs = nanos / 1_000_000_000;
    let nsec = (nanos % 1_000_000_000) as u32;
    let date = NaiveDateTime::from_timestamp_opt(secs, nsec).unwrap();
    DateTime::<Utc>::from_utc(date, Utc)
}

/// DATE_BIN sql function
pub fn date_bin(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() == 2 {
        // Default to unix EPOCH
        let origin = ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
            Some(0),
            Some("+00:00".into()),
        ));
        date_bin_impl(&args[0], &args[1], &origin)
    } else if args.len() == 3 {
        date_bin_impl(&args[0], &args[1], &args[2])
    } else {
        Err(DataFusionError::Execution(
            "DATE_BIN expected two or three arguments".to_string(),
        ))
    }
}

enum Interval {
    Nanoseconds(i64),
    Months(i64),
}

impl Interval {
    /// Returns (`stride_nanos`, `fn`) where
    ///
    /// 1. `stride_nanos` is a width, in nanoseconds
    /// 2. `fn` is a function that takes (stride_nanos, source, origin)
    ///
    /// `source` is the timestamp being binned
    ///
    /// `origin`  is the time, in nanoseconds, where windows are measured from
    fn bin_fn(&self) -> (i64, fn(i64, i64, i64) -> i64) {
        match self {
            Interval::Nanoseconds(nanos) => (*nanos, date_bin_nanos_interval),
            Interval::Months(months) => (*months, date_bin_months_interval),
        }
    }
}

// Supported intervals:
//  1. IntervalDayTime: this means that the stride is in days, hours, minutes, seconds and milliseconds
//     We will assume month interval won't be converted into this type
//     TODO (my next PR): without `INTERVAL` keyword, the stride was converted into ScalarValue::IntervalDayTime somwhere
//             for month interval. I need to find that and make it ScalarValue::IntervalMonthDayNano instead
// 2. IntervalMonthDayNano
fn date_bin_impl(
    stride: &ColumnarValue,
    array: &ColumnarValue,
    origin: &ColumnarValue,
) -> Result<ColumnarValue> {
    let stride = match stride {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos = (Duration::days(days as i64) + Duration::milliseconds(ms as i64))
                .num_nanoseconds();

            match nanos {
                Some(v) => Interval::Nanoseconds(v),
                _ => {
                    return Err(DataFusionError::Execution(
                        "DATE_BIN stride argument is too large".to_string(),
                    ))
                }
            }
        }
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            // If interval is months, its origin must be midnight of first date of the month
            if months != 0 {
                // Return error if days or nanos is not zero
                if days != 0 || nanos != 0 {
                    return Err(DataFusionError::NotImplemented(
                        "DATE_BIN stride does not support combination of month, day and nanosecond intervals".to_string(),
                    ));
                } else {
                    Interval::Months(months as i64)
                }
            } else {
                let nanos = (Duration::days(days as i64) + Duration::nanoseconds(nanos))
                    .num_nanoseconds();
                match nanos {
                    Some(v) => Interval::Nanoseconds(v),
                    _ => {
                        return Err(DataFusionError::Execution(
                            "DATE_BIN stride argument is too large".to_string(),
                        ))
                    }
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::Execution(format!(
                "DATE_BIN expects stride argument to be an INTERVAL but got {}",
                v.get_datatype()
            )))
        }
        ColumnarValue::Array(_) => return Err(DataFusionError::NotImplemented(
            "DATE_BIN only supports literal values for the stride argument, not arrays"
                .to_string(),
        )),
    };

    let origin = match origin {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::Execution(format!(
                "DATE_BIN expects origin argument to be a TIMESTAMP with nanosececond precision but got {}",
                v.get_datatype()
            )))
        }
        ColumnarValue::Array(_) => return Err(DataFusionError::NotImplemented(
            "DATE_BIN only supports literal values for the origin argument, not arrays"
                .to_string(),
        )),
    };

    let (stride, stride_fn) = stride.bin_fn();

    // Return error if stride is 0
    if stride == 0 {
        return Err(DataFusionError::Execution(
            "DATE_BIN stride must be non-zero".to_string(),
        ));
    }

    let f_nanos = |x: Option<i64>| x.map(|x| stride_fn(stride, x, origin));
    let f_micros = |x: Option<i64>| {
        let scale = 1_000;
        x.map(|x| stride_fn(stride, x * scale, origin) / scale)
    };
    let f_millis = |x: Option<i64>| {
        let scale = 1_000_000;
        x.map(|x| stride_fn(stride, x * scale, origin) / scale)
    };
    let f_secs = |x: Option<i64>| {
        let scale = 1_000_000_000;
        x.map(|x| stride_fn(stride, x * scale, origin) / scale)
    };

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                f_nanos(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                f_micros(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                f_millis(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                f_secs(*v),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let array = as_timestamp_nanosecond_array(array)?
                    .iter()
                    .map(f_nanos)
                    .collect::<TimestampNanosecondArray>();

                ColumnarValue::Array(Arc::new(array))
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = as_timestamp_microsecond_array(array)?
                    .iter()
                    .map(f_micros)
                    .collect::<TimestampMicrosecondArray>();

                ColumnarValue::Array(Arc::new(array))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = as_timestamp_millisecond_array(array)?
                    .iter()
                    .map(f_millis)
                    .collect::<TimestampMillisecondArray>();

                ColumnarValue::Array(Arc::new(array))
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                let array = as_timestamp_second_array(array)?
                    .iter()
                    .map(f_secs)
                    .collect::<TimestampSecondArray>();

                ColumnarValue::Array(Arc::new(array))
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "DATE_BIN expects source argument to be a TIMESTAMP but got {}",
                    array.data_type()
                )))
            }
        },
        _ => {
            return Err(DataFusionError::Execution(
                "DATE_BIN expects source argument to be a TIMESTAMP scalar or array"
                    .to_string(),
            ));
        }
    })
}

macro_rules! extract_date_part {
    ($ARRAY: expr, $FN:expr) => {
        match $ARRAY.data_type() {
            DataType::Date32 => {
                let array = as_date32_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Date64 => {
                let array = as_date64_array($ARRAY)?;
                Ok($FN(array)
                    .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => {
                    let array = as_timestamp_second_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Millisecond => {
                    let array = as_timestamp_millisecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Microsecond => {
                    let array = as_timestamp_microsecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
                TimeUnit::Nanosecond => {
                    let array = as_timestamp_nanosecond_array($ARRAY)?;
                    Ok($FN(array)
                        .map(|v| cast(&(Arc::new(v) as ArrayRef), &DataType::Float64))?)
                }
            },
            datatype => Err(DataFusionError::Internal(format!(
                "Extract does not support datatype {:?}",
                datatype
            ))),
        }
    };
}

/// DATE_PART SQL function
pub fn date_part(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(
            "Expected two arguments in DATE_PART".to_string(),
        ));
    }
    let (date_part, array) = (&args[0], &args[1]);

    let date_part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = date_part {
        v
    } else {
        return Err(DataFusionError::Execution(
            "First argument of `DATE_PART` must be non-null scalar Utf8".to_string(),
        ));
    };

    let is_scalar = matches!(array, ColumnarValue::Scalar(_));

    let array = match array {
        ColumnarValue::Array(array) => array.clone(),
        ColumnarValue::Scalar(scalar) => scalar.to_array(),
    };

    let arr = match date_part.to_lowercase().as_str() {
        "year" => extract_date_part!(&array, temporal::year),
        "quarter" => extract_date_part!(&array, temporal::quarter),
        "month" => extract_date_part!(&array, temporal::month),
        "week" => extract_date_part!(&array, temporal::week),
        "day" => extract_date_part!(&array, temporal::day),
        "doy" => extract_date_part!(&array, temporal::doy),
        "dow" => extract_date_part!(&array, temporal::num_days_from_sunday),
        "hour" => extract_date_part!(&array, temporal::hour),
        "minute" => extract_date_part!(&array, temporal::minute),
        "second" => extract_date_part!(&array, seconds),
        "millisecond" => extract_date_part!(&array, millis),
        "microsecond" => extract_date_part!(&array, micros),
        "nanosecond" => extract_date_part!(&array, nanos),
        "epoch" => extract_date_part!(&array, epoch),
        _ => Err(DataFusionError::Execution(format!(
            "Date part '{date_part}' not supported"
        ))),
    }?;

    Ok(if is_scalar {
        ColumnarValue::Scalar(ScalarValue::try_from_array(&arr?, 0)?)
    } else {
        ColumnarValue::Array(arr?)
    })
}

fn to_ticks<T>(array: &PrimitiveArray<T>, frac: i32) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let zipped = temporal::second(array)?
        .values()
        .iter()
        .zip(temporal::nanosecond(array)?.values().iter())
        .map(|o| ((*o.0 as f64 + (*o.1 as f64) / 1_000_000_000.0) * (frac as f64)))
        .collect::<Vec<f64>>();

    Ok(Float64Array::from(zipped))
}

fn seconds<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1)
}

fn millis<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000)
}

fn micros<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000)
}

fn nanos<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    to_ticks(array, 1_000_000_000)
}

fn epoch<T>(array: &PrimitiveArray<T>) -> Result<Float64Array>
where
    T: ArrowTemporalType + ArrowNumericType,
    i64: From<T::Native>,
{
    let mut b = Float64Builder::with_capacity(array.len());
    match array.data_type() {
        DataType::Timestamp(tu, _) => {
            for i in 0..array.len() {
                if array.is_null(i) {
                    b.append_null();
                } else {
                    let scale = match tu {
                        TimeUnit::Second => 1,
                        TimeUnit::Millisecond => 1_000,
                        TimeUnit::Microsecond => 1_000_000,
                        TimeUnit::Nanosecond => 1_000_000_000,
                    };

                    let n: i64 = array.value(i).into();
                    b.append_value(n as f64 / scale as f64);
                }
            }
        }
        _ => {
            return Err(DataFusionError::Internal(format!(
                "Can not convert {:?} to epoch",
                array.data_type()
            )))
        }
    }
    Ok(b.finish())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, IntervalDayTimeArray, StringBuilder};

    use super::*;

    #[test]
    fn to_timestamp_arrays_and_nulls() -> Result<()> {
        // ensure that arrow array implementation is wired up and handles nulls correctly

        let mut string_builder = StringBuilder::with_capacity(2, 1024);
        let mut ts_builder = TimestampNanosecondArray::builder(2);

        string_builder.append_value("2020-09-08T13:42:29.190855Z");
        ts_builder.append_value(1599572549190855000);

        string_builder.append_null();
        ts_builder.append_null();
        let expected_timestamps = &ts_builder.finish() as &dyn Array;

        let string_array =
            ColumnarValue::Array(Arc::new(string_builder.finish()) as ArrayRef);
        let parsed_timestamps = to_timestamp(&[string_array])
            .expect("that to_timestamp parsed values without error");
        if let ColumnarValue::Array(parsed_array) = parsed_timestamps {
            assert_eq!(parsed_array.len(), 2);
            assert_eq!(expected_timestamps, parsed_array.as_ref());
        } else {
            panic!("Expected a columnar array")
        }
        Ok(())
    }

    #[test]
    fn date_trunc_test() {
        let cases = vec![
            (
                "2020-09-08T13:42:29.190855Z",
                "second",
                "2020-09-08T13:42:29.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "minute",
                "2020-09-08T13:42:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "hour",
                "2020-09-08T13:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "day",
                "2020-09-08T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "week",
                "2020-09-07T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "month",
                "2020-09-01T00:00:00.000000Z",
            ),
            (
                "2020-09-08T13:42:29.190855Z",
                "year",
                "2020-01-01T00:00:00.000000Z",
            ),
            // week
            (
                "2021-01-01T13:42:29.190855Z",
                "week",
                "2020-12-28T00:00:00.000000Z",
            ),
            (
                "2020-01-01T13:42:29.190855Z",
                "week",
                "2019-12-30T00:00:00.000000Z",
            ),
            // quarter
            (
                "2020-01-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-02-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-03-01T13:42:29.190855Z",
                "quarter",
                "2020-01-01T00:00:00.000000Z",
            ),
            (
                "2020-04-01T13:42:29.190855Z",
                "quarter",
                "2020-04-01T00:00:00.000000Z",
            ),
            (
                "2020-08-01T13:42:29.190855Z",
                "quarter",
                "2020-07-01T00:00:00.000000Z",
            ),
            (
                "2020-11-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
            (
                "2020-12-01T13:42:29.190855Z",
                "quarter",
                "2020-10-01T00:00:00.000000Z",
            ),
        ];

        cases.iter().for_each(|(original, granularity, expected)| {
            let left = string_to_timestamp_nanos(original).unwrap();
            let right = string_to_timestamp_nanos(expected).unwrap();
            let result = date_trunc_coarse(granularity, left).unwrap();
            assert_eq!(result, right, "{original} = {expected}");
        });
    }

    #[test]
    fn test_date_bin_single() {
        use chrono::Duration;

        let cases = vec![
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2005-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    Duration::hours(1),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    Duration::seconds(10),
                    "2004-04-09T02:03:11.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:03:10Z",
            ),
        ];

        cases
            .iter()
            .for_each(|((stride, source, origin), expected)| {
                let stride1 = stride.num_nanoseconds().unwrap();
                let source1 = string_to_timestamp_nanos(source).unwrap();
                let origin1 = string_to_timestamp_nanos(origin).unwrap();

                let expected1 = string_to_timestamp_nanos(expected).unwrap();
                let result = date_bin_nanos_interval(stride1, source1, origin1);
                assert_eq!(result, expected1, "{source} = {expected}");
            })
    }

    #[test]
    fn test_date_bin() {
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // stride supports month-day-nano
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res =
            date_bin(&[ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1)))]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expected two or three arguments"
        );

        // stride: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects stride argument to be an INTERVAL but got Interval(YearMonth)"
        );

        // stride: invalid value
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(0))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN stride must be non-zero"
        );

        // stride: overflow of day-time interval
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(i64::MAX))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: overflow of month-day-nano interval
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, i32::MAX, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: month intervals
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1, 1, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "This feature is not implemented: DATE_BIN stride does not support combination of month, day and nanosecond intervals"
        );

        // origin: invalid type
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "Execution error: DATE_BIN expects origin argument to be a TIMESTAMP with nanosececond precision but got Timestamp(Microsecond, None)"
        );

        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // unsupported array type for stride
        let intervals = Arc::new((1..6).map(Some).collect::<IntervalDayTimeArray>());
        let res = date_bin(&[
            ColumnarValue::Array(intervals),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );

        // unsupported array type for origin
        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = date_bin(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Array(timestamps),
        ]);
        assert_eq!(
            res.err().unwrap().to_string(),
            "This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn to_timestamp_invalid_input_type() -> Result<()> {
        // pass the wrong type of input array to to_timestamp and test
        // that we get an error.

        let mut builder = Int64Array::builder(1);
        builder.append_value(1);
        let int64array = ColumnarValue::Array(Arc::new(builder.finish()));

        let expected_err =
            "Internal error: Unsupported data type Int64 for function to_timestamp";
        match to_timestamp(&[int64array]) {
            Ok(_) => panic!("Expected error but got success"),
            Err(e) => {
                assert!(
                    e.to_string().contains(expected_err),
                    "Can not find expected error '{expected_err}'. Actual error '{e}'"
                );
            }
        }
        Ok(())
    }
}
