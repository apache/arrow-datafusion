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

use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::types::ArrowTimestampType;
use arrow::array::{
    Array, ArrowPrimitiveType, GenericStringArray, OffsetSizeTrait, PrimitiveArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::error::ArrowError;
use chrono::LocalResult::Single;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use itertools::Either;

use datafusion_common::cast::as_generic_string_array;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarType, ScalarValue};
use datafusion_expr::ColumnarValue;

/// Error message if nanosecond conversion request beyond supported interval
const ERR_NANOSECONDS_NOT_SUPPORTED: &str = "The dates that can be represented as nanoseconds have to be between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804";

#[inline]
pub(crate) fn string_to_timestamp<T: ArrowTimestampType>(s: &str) -> Result<i64> {
    match T::UNIT {
        Second => Ok(x_string_to_datetime(&Utc, s)?.naive_utc().timestamp()),
        Millisecond => Ok(x_string_to_datetime(&Utc, s)?
            .naive_utc()
            .timestamp_millis()),
        Microsecond => Ok(x_string_to_datetime(&Utc, s)?
            .naive_utc()
            .timestamp_micros()),
        Nanosecond => {
            // Calls string_to_timestamp_nanos and converts the error type
            x_string_to_timestamp_nanos(s).map_err(|e| e.into())
        }
    }
}

/// Accepts a string with a `chrono` format and converts it to a
/// timestamp with precision corresponding to the specified timestamp type.
///
/// See [`chrono::format::strftime`] for the full set of supported formats.
///
/// Implements the `to_timestamp` function to convert a string to a
/// timestamp, following the model of spark SQL’s to_`timestamp`.
///
/// Internally, this function uses the `chrono` library for the
/// datetime parsing
///
/// ## Timestamp Precision
///
/// Function uses the maximum precision timestamps supported by
/// Arrow (nanoseconds stored as a 64-bit integer) timestamps. This
/// means the range of dates that (nanoseconds-precision) timestamps
/// can represent is ~1677 AD to 2262 AM
///
/// ## Timezone / Offset Handling
///
/// Numerical values of timestamps are stored compared to offset UTC.
///
/// Any timestamp in the formatting string is handled according to the rules
/// defined by `chrono`.
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
///
#[inline]
pub(crate) fn string_to_timestamp_formatted<T: ArrowTimestampType>(
    s: &str,
    format: &str,
) -> Result<i64, DataFusionError> {
    let shim = match T::UNIT {
        Second => |dt: &DateTime<_>| Ok(dt.timestamp()),
        Millisecond => |dt: &DateTime<_>| Ok(dt.timestamp_millis()),
        Microsecond => |dt: &DateTime<_>| Ok(dt.timestamp_micros()),
        Nanosecond => |dt: &DateTime<_>| {
            dt.timestamp_nanos_opt().ok_or_else(|| {
                DataFusionError::Execution(ERR_NANOSECONDS_NOT_SUPPORTED.to_string())
            })
        },
    };
    let dt = string_to_datetime_formatted(&Utc, s, format)?
        .naive_utc()
        .and_utc();
    shim(&dt)
}

/// TODO VT: the following should go to parse.rs

#[inline]
pub fn x_string_to_timestamp_nanos(s: &str) -> Result<i64, ArrowError> {
    x_to_timestamp_nanos(x_string_to_datetime(&Utc, s)?.naive_utc())
}

/// Fallible conversion of [`NaiveDateTime`] to `i64` nanoseconds
#[inline]
fn x_to_timestamp_nanos(dt: NaiveDateTime) -> Result<i64, ArrowError> {
    dt.timestamp_nanos_opt()
        .ok_or_else(|| ArrowError::ParseError(ERR_NANOSECONDS_NOT_SUPPORTED.to_string()))
}

fn x_string_to_datetime<T: TimeZone>(
    timezone: &T,
    s: &str,
) -> Result<DateTime<T>, ArrowError> {
    #[allow(deprecated)]
    const ZERO_TIME: NaiveTime = NaiveTime::from_hms(0, 0, 0);

    let err = |ctx: &str| {
        ArrowError::ParseError(format!("Error parsing timestamp from '{s}': {ctx}"))
    };

    let bytes = s.as_bytes();
    if bytes.len() < 10 {
        return Err(err("timestamp must contain at least 10 characters"));
    }

    let parser = XTimestampParser::new(bytes);
    let date = parser.date().ok_or_else(|| err("error parsing date"))?;
    if bytes.len() == 10 {
        let datetime = date.and_time(ZERO_TIME);
        return timezone
            .from_local_datetime(&datetime)
            .single()
            .ok_or_else(|| err("error computing timezone offset"));
    }

    let sep = bytes[10];
    if sep != b'T' && sep != b't' && sep != b' ' {
        return Err(err("invalid timestamp separator"));
    }

    let (time, mut tz_offset) = parser.time().ok_or_else(|| err("error parsing time"))?;
    let datetime = date.and_time(time);

    if tz_offset == 32 {
        // Decimal overrun
        while tz_offset < bytes.len() && bytes[tz_offset].is_ascii_digit() {
            tz_offset += 1;
        }
    }

    if bytes.len() <= tz_offset {
        return timezone
            .from_local_datetime(&datetime)
            .single()
            .ok_or_else(|| err("error computing timezone offset"));
    }

    let sep = bytes[tz_offset];
    if (sep == b'z' || sep == b'Z') && tz_offset == bytes.len() - 1 {
        return Ok(timezone.from_utc_datetime(&datetime));
    }

    // Parse remainder of string as timezone
    let parsed_tz: Tz = s[tz_offset..].trim_start().parse()?;

    let parsed = parsed_tz
        .from_local_datetime(&datetime)
        .single()
        .ok_or_else(|| err("error computing timezone offset"))?;

    Ok(parsed.with_timezone(timezone))
}

struct XTimestampParser {
    /// The timestamp bytes to parse minus `b'0'`
    ///
    /// This makes interpretation as an integer inexpensive
    digits: [u8; 32],
    /// A mask containing a `1` bit where the corresponding byte is a valid ASCII digit
    mask: u32,
}

impl XTimestampParser {
    fn new(bytes: &[u8]) -> Self {
        let mut digits = [0; 32];
        let mut mask = 0;

        // Treating all bytes the same way, helps LLVM vectorise this correctly
        for (idx, (o, i)) in digits.iter_mut().zip(bytes).enumerate() {
            *o = i.wrapping_sub(b'0');
            mask |= ((*o < 10) as u32) << idx
        }

        Self { digits, mask }
    }

    /// Parses a date of the form `1997-01-31`
    fn date(&self) -> Option<NaiveDate> {
        const DASH: u8 = b'-'.wrapping_sub(b'0');
        if self.mask & 0b1111111111 != 0b1101101111
            || self.digits[4] != DASH
            || self.digits[7] != DASH
        {
            return None;
        }

        let year = self.digits[0] as u16 * 1000
            + self.digits[1] as u16 * 100
            + self.digits[2] as u16 * 10
            + self.digits[3] as u16;

        let month = self.digits[5] * 10 + self.digits[6];
        let day = self.digits[8] * 10 + self.digits[9];

        NaiveDate::from_ymd_opt(year as _, month as _, day as _)
    }

    /// Parses a time of any of forms
    /// - `09:26:56`
    /// - `09:26:56.123`
    /// - `09:26:56.123456`
    /// - `09:26:56.123456789`
    /// - `092656`
    ///
    /// Returning the end byte offset
    fn time(&self) -> Option<(NaiveTime, usize)> {
        const PERIOD: u8 = b'.'.wrapping_sub(b'0');
        const COLON: u8 = b':'.wrapping_sub(b'0');

        // Make a NaiveTime handling leap seconds
        let time = |hour, min, sec, nano| match sec {
            60 => {
                let nano = 1_000_000_000 + nano;
                NaiveTime::from_hms_nano_opt(hour as _, min as _, 59, nano)
            }
            _ => NaiveTime::from_hms_nano_opt(hour as _, min as _, sec as _, nano),
        };

        match (self.mask >> 11) & 0b11111111 {
            // 09:26:56
            0b11011011 if self.digits[13] == COLON && self.digits[16] == COLON => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[14] * 10 + self.digits[15];
                let second = self.digits[17] * 10 + self.digits[18];

                if self.digits[19] == PERIOD {
                    let digits = (self.mask >> 20).trailing_ones();
                    let nanos = match digits {
                        0 => return None,
                        1 => x_parse_nanos::<1>(&self.digits[20..21]),
                        2 => x_parse_nanos::<2>(&self.digits[20..22]),
                        3 => x_parse_nanos::<3>(&self.digits[20..23]),
                        4 => x_parse_nanos::<4>(&self.digits[20..24]),
                        5 => x_parse_nanos::<5>(&self.digits[20..25]),
                        6 => x_parse_nanos::<6>(&self.digits[20..26]),
                        7 => x_parse_nanos::<7>(&self.digits[20..27]),
                        8 => x_parse_nanos::<8>(&self.digits[20..28]),
                        _ => x_parse_nanos::<9>(&self.digits[20..29]),
                    };
                    Some((time(hour, minute, second, nanos)?, 20 + digits as usize))
                } else {
                    Some((time(hour, minute, second, 0)?, 19))
                }
            }
            // 092656
            0b111111 => {
                let hour = self.digits[11] * 10 + self.digits[12];
                let minute = self.digits[13] * 10 + self.digits[14];
                let second = self.digits[15] * 10 + self.digits[16];
                let time = time(hour, minute, second, 0)?;
                Some((time, 17))
            }
            _ => None,
        }
    }
}

#[inline]
fn x_parse_nanos<const N: usize>(digits: &[u8]) -> u32 {
    digits[..N]
        .iter()
        .fold(0_u32, |acc, v| acc * 10 + *v as u32)
        * 10_u32.pow((9 - N) as _)
}

/////////////////////

/// Checks that all the arguments from the second are of type [Utf8] or [LargeUtf8]
///
/// [Utf8]: DataType::Utf8
/// [LargeUtf8]: DataType::LargeUtf8
pub(crate) fn validate_data_types(args: &[ColumnarValue], name: &str) -> Result<()> {
    for (idx, a) in args.iter().skip(1).enumerate() {
        match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // all good
            }
            _ => {
                return exec_err!(
                    "{name} function unsupported data type at index {}: {}",
                    idx + 1,
                    a.data_type()
                );
            }
        }
    }

    Ok(())
}

/// Accepts a string and parses it using the [`chrono::format::strftime`] specifiers
/// relative to the provided `timezone`
///
/// [IANA timezones] are only supported if the `arrow-array/chrono-tz` feature is enabled
///
/// * `2023-01-01 040506 America/Los_Angeles`
///
/// If a timestamp is ambiguous, for example as a result of daylight-savings time, an error
/// will be returned
///
/// [`chrono::format::strftime`]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html
/// [IANA timezones]: https://www.iana.org/time-zones
pub(crate) fn string_to_datetime_formatted<T: TimeZone>(
    timezone: &T,
    s: &str,
    format: &str,
) -> Result<DateTime<T>, DataFusionError> {
    let err = |err_ctx: &str| {
        DataFusionError::Execution(format!(
            "Error parsing timestamp from '{s}' using format '{format}': {err_ctx}"
        ))
    };

    // attempt to parse the string assuming it has a timezone
    let dt = DateTime::parse_from_str(s, format);

    if let Err(e) = &dt {
        // no timezone or other failure, try without a timezone
        let ndt = NaiveDateTime::parse_from_str(s, format);
        if let Err(e) = &ndt {
            return Err(err(&e.to_string()));
        }

        if let Single(e) = &timezone.from_local_datetime(&ndt.unwrap()) {
            Ok(e.to_owned())
        } else {
            Err(err(&e.to_string()))
        }
    } else {
        Ok(dt.unwrap().with_timezone(timezone))
    }
}

pub(crate) fn handle<'a, O, F, S>(
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
            DataType::Utf8 | DataType::LargeUtf8 => Ok(ColumnarValue::Array(Arc::new(
                unary_string_to_primitive_function::<i32, O, _>(&[a.as_ref()], op, name)?,
            ))),
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let result = a.as_ref().map(|x| (op)(x)).transpose()?;
                Ok(ColumnarValue::Scalar(S::scalar(result)))
            }
            other => exec_err!("Unsupported data type {other:?} for function {name}"),
        },
    }
}

// given an function that maps a `&str`, `&str` to an arrow native type,
// returns a `ColumnarValue` where the function is applied to either a `ArrayRef` or `ScalarValue`
// depending on the `args`'s variant.
pub(crate) fn handle_multiple<'a, O, F, S>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<ColumnarValue>
where
    O: ArrowPrimitiveType,
    S: ScalarType<O::Native>,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
{
    match &args[0] {
        ColumnarValue::Array(a) => match a.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => {
                // validate the column types
                for (pos, arg) in args.iter().enumerate() {
                    match arg {
                        ColumnarValue::Array(arg) => match arg.data_type() {
                            DataType::Utf8 | DataType::LargeUtf8 => {
                                // all good
                            }
                            other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                        },
                        ColumnarValue::Scalar(arg) => {
                            match arg.data_type() {
                                DataType::Utf8 | DataType::LargeUtf8 => {
                                    // all good
                                }
                                other => return exec_err!("Unsupported data type {other:?} for function {name}, arg # {pos}"),
                            }
                        }
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(
                    strings_to_primitive_function::<i32, O, _>(args, op, name)?,
                )))
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
        // if the first argument is a scalar utf8 all arguments are expected to be scalar utf8
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => {
                let mut val: Option<Result<ColumnarValue>> = None;
                let mut err: Option<DataFusionError> = None;

                match a {
                    Some(a) => {
                        // enumerate all the values finding the first one that returns an Ok result
                        for (pos, v) in args.iter().enumerate().skip(1) {
                            if let ColumnarValue::Scalar(s) = v {
                                if let ScalarValue::Utf8(x) | ScalarValue::LargeUtf8(x) =
                                    s
                                {
                                    if let Some(s) = x {
                                        match op(a.as_str(), s.as_str()) {
                                            Ok(r) => {
                                                val = Some(Ok(ColumnarValue::Scalar(
                                                    S::scalar(Some(r)),
                                                )));
                                                break;
                                            }
                                            Err(e) => {
                                                err = Some(e);
                                            }
                                        }
                                    }
                                } else {
                                    return exec_err!("Unsupported data type {s:?} for function {name}, arg # {pos}");
                                }
                            } else {
                                return exec_err!("Unsupported data type {v:?} for function {name}, arg # {pos}");
                            }
                        }
                    }
                    None => (),
                }

                if let Some(v) = val {
                    v
                } else {
                    Err(err.unwrap())
                }
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function {name}")
            }
        },
    }
}

/// given a function `op` that maps `&str`, `&str` to the first successful Result
/// of an arrow native type, returns a `PrimitiveArray` after the application of the
/// function to `args`. This function calls the `op` function with the first and second
/// argument and if not successful continues with first and third, first and fourth,
/// etc until the result was successful or no more arguments are present.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not > 1 or
/// * the array arguments are not castable to a `GenericStringArray` or
/// * the function `op` errors for all input
pub(crate) fn strings_to_primitive_function<'a, T, O, F>(
    args: &'a [ColumnarValue],
    op: F,
    name: &str,
) -> Result<PrimitiveArray<O>>
where
    O: ArrowPrimitiveType,
    T: OffsetSizeTrait,
    F: Fn(&'a str, &'a str) -> Result<O::Native>,
{
    if args.len() < 2 {
        return exec_err!(
            "{:?} args were supplied but {} takes 2 or more arguments",
            args.len(),
            name
        );
    }

    // this will throw the error if any of the array args are not castable to GenericStringArray
    let data = args
        .iter()
        .map(|a| match a {
            ColumnarValue::Array(a) => {
                Ok(Either::Left(as_generic_string_array::<T>(a.as_ref())?))
            }
            ColumnarValue::Scalar(s) => match s {
                ScalarValue::Utf8(a) | ScalarValue::LargeUtf8(a) => Ok(Either::Right(a)),
                other => exec_err!(
                    "Unexpected scalar type encountered '{other}' for function '{name}'"
                ),
            },
        })
        .collect::<Result<Vec<Either<&GenericStringArray<T>, &Option<String>>>>>()?;

    let first_arg = &data.first().unwrap().left().unwrap();

    first_arg
        .iter()
        .enumerate()
        .map(|(pos, x)| {
            let mut val = None;

            if let Some(x) = x {
                let param_args = data.iter().skip(1);

                // go through the args and find the first successful result. Only the last
                // failure will be returned if no successful result was received.
                for param_arg in param_args {
                    // param_arg is an array, use the corresponding index into the array as the arg
                    // we're currently parsing
                    let p = *param_arg;
                    let r = if p.is_left() {
                        let p = p.left().unwrap();
                        op(x, p.value(pos))
                    }
                    // args is a scalar, use it directly
                    else if let Some(p) = p.right().unwrap() {
                        op(x, p.as_str())
                    } else {
                        continue;
                    };

                    if r.is_ok() {
                        val = Some(r);
                        break;
                    } else {
                        val = Some(r);
                    }
                }
            };

            val.transpose()
        })
        .collect()
}

/// given a function `op` that maps a `&str` to a Result of an arrow native type,
/// returns a `PrimitiveArray` after the application
/// of the function to `args[0]`.
/// # Errors
/// This function errors iff:
/// * the number of arguments is not 1 or
/// * the first argument is not castable to a `GenericStringArray` or
/// * the function `op` errors
fn unary_string_to_primitive_function<'a, T, O, F>(
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
        return exec_err!(
            "{:?} args were supplied but {} takes exactly one argument",
            args.len(),
            name
        );
    }

    let array = as_generic_string_array::<T>(args[0])?;

    // first map is the iterator, second is for the `Option<_>`
    array.iter().map(|x| x.map(&op).transpose()).collect()
}
