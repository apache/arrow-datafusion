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

use std::any::Any;
use std::sync::Arc;

use arrow::array::temporal_conversions::NANOSECONDS;
use arrow::array::types::{
    ArrowTimestampType, IntervalDayTimeType, IntervalMonthDayNanoType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::datatypes::DataType::{Null, Timestamp, Utf8};
use arrow::datatypes::IntervalUnit::{DayTime, MonthDayNano};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, TimeUnit};

use datafusion_common::cast::as_primitive_array;
use datafusion_common::{exec_err, not_impl_err, plan_err, Result, ScalarValue};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

use chrono::{DateTime, Datelike, Duration, Months, TimeDelta, Utc};

#[derive(Debug)]
pub struct DateBinFunc {
    signature: Signature,
}

impl Default for DateBinFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DateBinFunc {
    pub fn new() -> Self {
        let base_sig = |array_type: TimeUnit| {
            vec![
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, None),
                    Timestamp(Nanosecond, None),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                    Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, None),
                    Timestamp(Nanosecond, None),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                    Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, None),
                ]),
                Exact(vec![
                    DataType::Interval(MonthDayNano),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, None),
                ]),
                Exact(vec![
                    DataType::Interval(DayTime),
                    Timestamp(array_type, Some(TIMEZONE_WILDCARD.into())),
                ]),
            ]
        };

        let full_sig = [Nanosecond, Microsecond, Millisecond, Second]
            .into_iter()
            .map(base_sig)
            .collect::<Vec<_>>()
            .concat();

        Self {
            signature: Signature::one_of(full_sig, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DateBinFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_bin"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[1] {
            Timestamp(Nanosecond, None) | Utf8 | Null => Ok(Timestamp(Nanosecond, None)),
            Timestamp(Nanosecond, tz_opt) => Ok(Timestamp(Nanosecond, tz_opt.clone())),
            Timestamp(Microsecond, tz_opt) => Ok(Timestamp(Microsecond, tz_opt.clone())),
            Timestamp(Millisecond, tz_opt) => Ok(Timestamp(Millisecond, tz_opt.clone())),
            Timestamp(Second, tz_opt) => Ok(Timestamp(Second, tz_opt.clone())),
            _ => plan_err!(
                "The date_bin function can only accept timestamp as the second arg."
            ),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
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
            exec_err!("DATE_BIN expected two or three arguments")
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        // The DATE_BIN function preserves the order of its second argument.
        let step = &input[0];
        let date_value = &input[1];
        let reference = input.get(2);

        if step.sort_properties.eq(&SortProperties::Singleton)
            && reference
                .map(|r| r.sort_properties.eq(&SortProperties::Singleton))
                .unwrap_or(true)
        {
            Ok(date_value.sort_properties)
        } else {
            Ok(SortProperties::Unordered)
        }
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

    bin_time.timestamp_nanos_opt().unwrap()
}

fn to_utc_date_time(nanos: i64) -> DateTime<Utc> {
    let secs = nanos / 1_000_000_000;
    let nsec = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsec).unwrap()
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
            let nanos = (TimeDelta::try_days(days as i64).unwrap()
                + TimeDelta::try_milliseconds(ms as i64).unwrap())
            .num_nanoseconds();

            match nanos {
                Some(v) => Interval::Nanoseconds(v),
                _ => return exec_err!("DATE_BIN stride argument is too large"),
            }
        }
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            // If interval is months, its origin must be midnight of first date of the month
            if months != 0 {
                // Return error if days or nanos is not zero
                if days != 0 || nanos != 0 {
                    return not_impl_err!(
                        "DATE_BIN stride does not support combination of month, day and nanosecond intervals"
                    );
                } else {
                    Interval::Months(months as i64)
                }
            } else {
                let nanos = (TimeDelta::try_days(days as i64).unwrap()
                    + Duration::nanoseconds(nanos))
                .num_nanoseconds();
                match nanos {
                    Some(v) => Interval::Nanoseconds(v),
                    _ => return exec_err!("DATE_BIN stride argument is too large"),
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects stride argument to be an INTERVAL but got {}",
                v.data_type()
            );
        }
        ColumnarValue::Array(_) => {
            return not_impl_err!(
            "DATE_BIN only supports literal values for the stride argument, not arrays"
        );
        }
    };

    let origin = match origin {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        ColumnarValue::Scalar(v) => {
            return exec_err!(
                "DATE_BIN expects origin argument to be a TIMESTAMP with nanosecond precision but got {}",
                v.data_type()
            );
        }
        ColumnarValue::Array(_) => {
            return not_impl_err!(
            "DATE_BIN only supports literal values for the origin argument, not arrays"
        );
        }
    };

    let (stride, stride_fn) = stride.bin_fn();

    // Return error if stride is 0
    if stride == 0 {
        return exec_err!("DATE_BIN stride must be non-zero");
    }

    fn stride_map_fn<T: ArrowTimestampType>(
        origin: i64,
        stride: i64,
        stride_fn: fn(i64, i64, i64) -> i64,
    ) -> impl Fn(i64) -> i64 {
        let scale = match T::UNIT {
            Nanosecond => 1,
            Microsecond => NANOSECONDS / 1_000_000,
            Millisecond => NANOSECONDS / 1_000,
            Second => NANOSECONDS,
        };
        move |x: i64| stride_fn(stride, x * scale, origin) / scale
    }

    Ok(match array {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampNanosecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                v.map(apply_stride_fn),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampMicrosecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                v.map(apply_stride_fn),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampMillisecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
                v.map(apply_stride_fn),
                tz_opt.clone(),
            ))
        }
        ColumnarValue::Scalar(ScalarValue::TimestampSecond(v, tz_opt)) => {
            let apply_stride_fn =
                stride_map_fn::<TimestampSecondType>(origin, stride, stride_fn);
            ColumnarValue::Scalar(ScalarValue::TimestampSecond(
                v.map(apply_stride_fn),
                tz_opt.clone(),
            ))
        }

        ColumnarValue::Array(array) => {
            fn transform_array_with_stride<T>(
                origin: i64,
                stride: i64,
                stride_fn: fn(i64, i64, i64) -> i64,
                array: &ArrayRef,
                tz_opt: &Option<Arc<str>>,
            ) -> Result<ColumnarValue>
            where
                T: ArrowTimestampType,
            {
                let array = as_primitive_array::<T>(array)?;
                let apply_stride_fn = stride_map_fn::<T>(origin, stride, stride_fn);
                let array: PrimitiveArray<T> = array
                    .unary(apply_stride_fn)
                    .with_timezone_opt(tz_opt.clone());

                Ok(ColumnarValue::Array(Arc::new(array)))
            }

            match array.data_type() {
                Timestamp(Nanosecond, tz_opt) => {
                    transform_array_with_stride::<TimestampNanosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Microsecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMicrosecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Millisecond, tz_opt) => {
                    transform_array_with_stride::<TimestampMillisecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                Timestamp(Second, tz_opt) => {
                    transform_array_with_stride::<TimestampSecondType>(
                        origin, stride, stride_fn, array, tz_opt,
                    )?
                }
                _ => {
                    return exec_err!(
                        "DATE_BIN expects source argument to be a TIMESTAMP but got {}",
                        array.data_type()
                    );
                }
            }
        }
        _ => {
            return exec_err!(
                "DATE_BIN expects source argument to be a TIMESTAMP scalar or array"
            );
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::datetime::date_bin::{date_bin_nanos_interval, DateBinFunc};
    use arrow::array::types::TimestampNanosecondType;
    use arrow::array::{IntervalDayTimeArray, TimestampNanosecondArray};
    use arrow::compute::kernels::cast_utils::string_to_timestamp_nanos;
    use arrow::datatypes::{DataType, TimeUnit};

    use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    use chrono::TimeDelta;

    #[test]
    fn test_date_bin() {
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Array(timestamps),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // stride supports month-day-nano
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano {
                    months: 0,
                    days: 0,
                    nanoseconds: 1,
                },
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let res = DateBinFunc::new().invoke(&[ColumnarValue::Scalar(
            ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            })),
        )]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expected two or three arguments"
        );

        // stride: invalid type
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalYearMonth(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects stride argument to be an INTERVAL but got Interval(YearMonth)"
        );

        // stride: invalid value
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 0,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride must be non-zero"
        );

        // stride: overflow of day-time interval
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(
                IntervalDayTime::MAX,
            ))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: overflow of month-day-nano interval
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, i32::MAX, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN stride argument is too large"
        );

        // stride: month intervals
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(1, 1, 1)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN stride does not support combination of month, day and nanosecond intervals"
        );

        // origin: invalid type
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "Execution error: DATE_BIN expects origin argument to be a TIMESTAMP with nanosecond precision but got Timestamp(Microsecond, None)"
        );

        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert!(res.is_ok());

        // unsupported array type for stride
        let intervals = Arc::new(
            (1..6)
                .map(|x| {
                    Some(IntervalDayTime {
                        days: 0,
                        milliseconds: x,
                    })
                })
                .collect::<IntervalDayTimeArray>(),
        );
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Array(intervals),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the stride argument, not arrays"
        );

        // unsupported array type for origin
        let timestamps = Arc::new((1..6).map(Some).collect::<TimestampNanosecondArray>());
        let res = DateBinFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                days: 0,
                milliseconds: 1,
            }))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
            ColumnarValue::Array(timestamps),
        ]);
        assert_eq!(
            res.err().unwrap().strip_backtrace(),
            "This feature is not implemented: DATE_BIN only supports literal values for the origin argument, not arrays"
        );
    }

    #[test]
    fn test_date_bin_timezones() {
        let cases = vec![
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("+00".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                None,
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T01:00:00Z",
                    "2020-09-08T02:00:00Z",
                    "2020-09-08T03:00:00Z",
                    "2020-09-08T04:00:00Z",
                ],
                Some("-02".into()),
                "1970-01-01T00:00:00Z",
                vec![
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                    "2020-09-08T00:00:00Z",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T01:00:00+05",
                    "2020-09-08T02:00:00+05",
                    "2020-09-08T03:00:00+05",
                    "2020-09-08T04:00:00+05",
                ],
                Some("+05".into()),
                "1970-01-01T00:00:00+05",
                vec![
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                    "2020-09-08T00:00:00+05",
                ],
            ),
            (
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T01:00:00+08",
                    "2020-09-08T02:00:00+08",
                    "2020-09-08T03:00:00+08",
                    "2020-09-08T04:00:00+08",
                ],
                Some("+08".into()),
                "1970-01-01T00:00:00+08",
                vec![
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                    "2020-09-08T00:00:00+08",
                ],
            ),
        ];

        cases
            .iter()
            .for_each(|(original, tz_opt, origin, expected)| {
                let input = original
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let right = expected
                    .iter()
                    .map(|s| Some(string_to_timestamp_nanos(s).unwrap()))
                    .collect::<TimestampNanosecondArray>()
                    .with_timezone_opt(tz_opt.clone());
                let result = DateBinFunc::new()
                    .invoke(&[
                        ColumnarValue::Scalar(ScalarValue::new_interval_dt(1, 0)),
                        ColumnarValue::Array(Arc::new(input)),
                        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(
                            Some(string_to_timestamp_nanos(origin).unwrap()),
                            tz_opt.clone(),
                        )),
                    ])
                    .unwrap();
                if let ColumnarValue::Array(result) = result {
                    assert_eq!(
                        result.data_type(),
                        &DataType::Timestamp(TimeUnit::Nanosecond, tz_opt.clone())
                    );
                    let left = arrow::array::cast::as_primitive_array::<
                        TimestampNanosecondType,
                    >(&result);
                    assert_eq!(left, &right);
                } else {
                    panic!("unexpected column type");
                }
            });
    }

    #[test]
    fn test_date_bin_single() {
        let cases = vec![
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    TimeDelta::try_minutes(15),
                    "2004-04-09T02:03:04.123456789Z",
                    "2005-01-01T00:02:30",
                ),
                "2004-04-09T02:02:30Z",
            ),
            (
                (
                    TimeDelta::try_hours(1),
                    "2004-04-09T02:03:04.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:00:00Z",
            ),
            (
                (
                    TimeDelta::try_seconds(10),
                    "2004-04-09T02:03:11.123456789Z",
                    "2001-01-01T00:00:00",
                ),
                "2004-04-09T02:03:10Z",
            ),
        ];

        cases
            .iter()
            .for_each(|((stride, source, origin), expected)| {
                let stride = stride.unwrap();
                let stride1 = stride.num_nanoseconds().unwrap();
                let source1 = string_to_timestamp_nanos(source).unwrap();
                let origin1 = string_to_timestamp_nanos(origin).unwrap();

                let expected1 = string_to_timestamp_nanos(expected).unwrap();
                let result = date_bin_nanos_interval(stride1, source1, origin1);
                assert_eq!(result, expected1, "{source} = {expected}");
            })
    }
}
