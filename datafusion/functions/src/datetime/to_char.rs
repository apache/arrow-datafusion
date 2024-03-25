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

use arrow::array::cast::AsArray;
use arrow::array::{new_null_array, Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{
    Date32, Date64, Duration, Time32, Time64, Timestamp, Utf8,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::error::ArrowError;
use arrow::util::display::{ArrayFormatter, DurationFormat, FormatOptions};

use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

#[derive(Debug)]
pub(super) struct ToCharFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl ToCharFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Date32, Utf8]),
                    Exact(vec![Date64, Utf8]),
                    Exact(vec![Time32(Millisecond), Utf8]),
                    Exact(vec![Time32(Second), Utf8]),
                    Exact(vec![Time64(Microsecond), Utf8]),
                    Exact(vec![Time64(Nanosecond), Utf8]),
                    Exact(vec![Timestamp(Second, None), Utf8]),
                    Exact(vec![
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Millisecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Microsecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Timestamp(Nanosecond, None), Utf8]),
                    Exact(vec![
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                        Utf8,
                    ]),
                    Exact(vec![Duration(Second), Utf8]),
                    Exact(vec![Duration(Millisecond), Utf8]),
                    Exact(vec![Duration(Microsecond), Utf8]),
                    Exact(vec![Duration(Nanosecond), Utf8]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("date_format")],
        }
    }
}

impl ScalarUDFImpl for ToCharFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "to_char"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!(
                "to_char function requires 2 arguments, got {}",
                args.len()
            );
        }

        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(None))
            | ColumnarValue::Scalar(ScalarValue::Null) => {
                _to_char_scalar(args[0].clone(), None)
            }
            // constant format
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(format))) => {
                // invoke to_char_scalar with the known string, without converting to array
                _to_char_scalar(args[0].clone(), Some(format))
            }
            ColumnarValue::Array(_) => _to_char_array(args),
            _ => {
                exec_err!(
                    "Format for `to_char` must be non-null Utf8, received {:?}",
                    args[1].data_type()
                )
            }
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

fn _build_format_options<'a>(
    data_type: &DataType,
    format: Option<&'a str>,
) -> Result<FormatOptions<'a>, Result<ColumnarValue>> {
    let Some(format) = format else {
        return Ok(FormatOptions::new());
    };
    let format_options = match data_type {
        Date32 => FormatOptions::new().with_date_format(Some(format)),
        Date64 => FormatOptions::new().with_datetime_format(Some(format)),
        Time32(_) => FormatOptions::new().with_time_format(Some(format)),
        Time64(_) => FormatOptions::new().with_time_format(Some(format)),
        Timestamp(_, _) => FormatOptions::new()
            .with_timestamp_format(Some(format))
            .with_timestamp_tz_format(Some(format)),
        Duration(_) => FormatOptions::new().with_duration_format(
            if "ISO8601".eq_ignore_ascii_case(format) {
                DurationFormat::ISO8601
            } else {
                DurationFormat::Pretty
            },
        ),
        other => {
            return Err(exec_err!(
                "to_char only supports date, time, timestamp and duration data types, received {other:?}"
            ));
        }
    };
    Ok(format_options)
}

/// Special version when arg\[1] is a scalar
fn _to_char_scalar(
    expression: ColumnarValue,
    format: Option<&str>,
) -> Result<ColumnarValue> {
    // it's possible that the expression is a scalar however because
    // of the implementation in arrow-rs we need to convert it to an array
    let data_type = &expression.data_type();
    let is_scalar_expression = matches!(&expression, ColumnarValue::Scalar(_));
    let array = expression.into_array(1)?;

    if format.is_none() {
        if is_scalar_expression {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        } else {
            return Ok(ColumnarValue::Array(new_null_array(
                &DataType::Utf8,
                array.len(),
            )));
        }
    }

    let format_options = match _build_format_options(data_type, format) {
        Ok(value) => value,
        Err(value) => return value,
    };

    let formatter = ArrayFormatter::try_new(array.as_ref(), &format_options)?;
    let formatted: Result<Vec<_>, ArrowError> = (0..array.len())
        .map(|i| formatter.value(i).try_to_string())
        .collect();

    if let Ok(formatted) = formatted {
        if is_scalar_expression {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                formatted.first().unwrap().to_string(),
            ))))
        } else {
            Ok(ColumnarValue::Array(
                Arc::new(StringArray::from(formatted)) as ArrayRef
            ))
        }
    } else {
        exec_err!("{}", formatted.unwrap_err())
    }
}

fn _to_char_array(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays = ColumnarValue::values_to_arrays(args)?;
    let mut results: Vec<Option<String>> = vec![];
    let format_array = arrays[1].as_string::<i32>();
    let data_type = arrays[0].data_type();

    for idx in 0..arrays[0].len() {
        let format = if format_array.is_null(idx) {
            None
        } else {
            Some(format_array.value(idx))
        };
        if format.is_none() {
            results.push(None);
            continue;
        }
        let format_options = match _build_format_options(data_type, format) {
            Ok(value) => value,
            Err(value) => return value,
        };
        // this isn't ideal but this can't use ValueFormatter as it isn't independent
        // from ArrayFormatter
        let formatter = ArrayFormatter::try_new(arrays[0].as_ref(), &format_options)?;
        let result = formatter.value(idx).try_to_string();
        match result {
            Ok(value) => results.push(Some(value)),
            Err(e) => return exec_err!("{}", e),
        }
    }

    match args[0] {
        ColumnarValue::Array(_) => Ok(ColumnarValue::Array(Arc::new(StringArray::from(
            results,
        )) as ArrayRef)),
        ColumnarValue::Scalar(_) => match results.first().unwrap() {
            Some(value) => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                value.to_string(),
            )))),
            None => Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::datetime::to_char::ToCharFunc;
    use arrow::array::{
        Array, ArrayRef, Date32Array, Date64Array, StringArray, Time32MillisecondArray,
        Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use chrono::{NaiveDateTime, Timelike};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};
    use std::sync::Arc;

    #[test]
    fn test_to_char() {
        let date = "2020-01-02T03:04:05"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(12345)
            .unwrap();
        let date2 = "2026-07-08T09:10:11"
            .parse::<NaiveDateTime>()
            .unwrap()
            .with_nanosecond(56789)
            .unwrap();

        let scalar_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.and_utc().timestamp_millis())),
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                ScalarValue::Utf8(Some("%H-%M-%S".to_string())),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                ScalarValue::Utf8(Some("%H-%M-%S %f".to_string())),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.and_utc().timestamp()), None),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(
                    Some(date.and_utc().timestamp_millis()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H".to_string())),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(date.and_utc().timestamp_micros()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.and_utc().timestamp_nanos_opt().unwrap()),
                    None,
                ),
                ScalarValue::Utf8(Some("%Y::%m::%d %S::%M::%H %f".to_string())),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_data {
            let result = ToCharFunc::new()
                .invoke(&[ColumnarValue::Scalar(value), ColumnarValue::Scalar(format)])
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let scalar_array_data = vec![
            (
                ScalarValue::Date32(Some(18506)),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::09::01".to_string(),
            ),
            (
                ScalarValue::Date64(Some(date.and_utc().timestamp_millis())),
                StringArray::from(vec!["%Y::%m::%d".to_string()]),
                "2020::01::02".to_string(),
            ),
            (
                ScalarValue::Time32Second(Some(31851)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "08-50-51".to_string(),
            ),
            (
                ScalarValue::Time32Millisecond(Some(18506000)),
                StringArray::from(vec!["%H-%M-%S".to_string()]),
                "05-08-26".to_string(),
            ),
            (
                ScalarValue::Time64Microsecond(Some(12344567000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567000000".to_string(),
            ),
            (
                ScalarValue::Time64Nanosecond(Some(12344567890000)),
                StringArray::from(vec!["%H-%M-%S %f".to_string()]),
                "03-25-44 567890000".to_string(),
            ),
            (
                ScalarValue::TimestampSecond(Some(date.and_utc().timestamp()), None),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMillisecond(
                    Some(date.and_utc().timestamp_millis()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H".to_string()]),
                "2020::01::02 05::04::03".to_string(),
            ),
            (
                ScalarValue::TimestampMicrosecond(
                    Some(date.and_utc().timestamp_micros()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012000".to_string(),
            ),
            (
                ScalarValue::TimestampNanosecond(
                    Some(date.and_utc().timestamp_nanos_opt().unwrap()),
                    None,
                ),
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H %f".to_string()]),
                "2020::01::02 05::04::03 000012345".to_string(),
            ),
        ];

        for (value, format, expected) in scalar_array_data {
            let result = ToCharFunc::new()
                .invoke(&[
                    ColumnarValue::Scalar(value),
                    ColumnarValue::Array(Arc::new(format) as ArrayRef),
                ])
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Scalar(ScalarValue::Utf8(date)) = result {
                assert_eq!(expected, date.unwrap());
            } else {
                panic!("Expected a scalar value")
            }
        }

        let array_scalar_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::09::01", "2020::09::02"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                ScalarValue::Utf8(Some("%Y::%m::%d".to_string())),
                StringArray::from(vec!["2020::01::02", "2026::07::08"]),
            ),
        ];

        let array_array_data = vec![
            (
                Arc::new(Date32Array::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::09::01", "02::09::2020"]),
            ),
            (
                Arc::new(Date64Array::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d", "%d::%m::%Y"]),
                StringArray::from(vec!["2020::01::02", "08::07::2026"]),
            ),
            (
                Arc::new(Time32MillisecondArray::from(vec![1850600, 1860700]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:30:50", "00::31::00"]),
            ),
            (
                Arc::new(Time32SecondArray::from(vec![18506, 18507])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["05:08:26", "05::08::27"]),
            ),
            (
                Arc::new(Time64MicrosecondArray::from(vec![12344567000, 22244567000]))
                    as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["03:25:44", "06::10::44"]),
            ),
            (
                Arc::new(Time64NanosecondArray::from(vec![
                    1234456789000,
                    2224456789000,
                ])) as ArrayRef,
                StringArray::from(vec!["%H:%M:%S", "%H::%M::%S"]),
                StringArray::from(vec!["00:20:34", "00::37::04"]),
            ),
            (
                Arc::new(TimestampSecondArray::from(vec![
                    date.and_utc().timestamp(),
                    date2.and_utc().timestamp(),
                ])) as ArrayRef,
                StringArray::from(vec!["%Y::%m::%d %S::%M::%H", "%d::%m::%Y %S-%M-%H"]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03",
                    "08::07::2026 11-10-09",
                ]),
            ),
            (
                Arc::new(TimestampMillisecondArray::from(vec![
                    date.and_utc().timestamp_millis(),
                    date2.and_utc().timestamp_millis(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000000000",
                    "08::07::2026 11-10-09 000000000",
                ]),
            ),
            (
                Arc::new(TimestampMicrosecondArray::from(vec![
                    date.and_utc().timestamp_micros(),
                    date2.and_utc().timestamp_micros(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012000",
                    "08::07::2026 11-10-09 000056000",
                ]),
            ),
            (
                Arc::new(TimestampNanosecondArray::from(vec![
                    date.and_utc().timestamp_nanos_opt().unwrap(),
                    date2.and_utc().timestamp_nanos_opt().unwrap(),
                ])) as ArrayRef,
                StringArray::from(vec![
                    "%Y::%m::%d %S::%M::%H %f",
                    "%d::%m::%Y %S-%M-%H %f",
                ]),
                StringArray::from(vec![
                    "2020::01::02 05::04::03 000012345",
                    "08::07::2026 11-10-09 000056789",
                ]),
            ),
        ];

        for (value, format, expected) in array_scalar_data {
            let result = ToCharFunc::new()
                .invoke(&[
                    ColumnarValue::Array(value as ArrayRef),
                    ColumnarValue::Scalar(format),
                ])
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        for (value, format, expected) in array_array_data {
            let result = ToCharFunc::new()
                .invoke(&[
                    ColumnarValue::Array(value),
                    ColumnarValue::Array(Arc::new(format) as ArrayRef),
                ])
                .expect("that to_char parsed values without error");

            if let ColumnarValue::Array(result) = result {
                assert_eq!(result.len(), 2);
                assert_eq!(&expected as &dyn Array, result.as_ref());
            } else {
                panic!("Expected an array value")
            }
        }

        //
        // Fallible test cases
        //

        // invalid number of arguments
        let result = ToCharFunc::new()
            .invoke(&[ColumnarValue::Scalar(ScalarValue::Int32(Some(1)))]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: to_char function requires 2 arguments, got 1"
        );

        // invalid type
        let result = ToCharFunc::new().invoke(&[
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ]);
        assert_eq!(
            result.err().unwrap().strip_backtrace(),
            "Execution error: Format for `to_char` must be non-null Utf8, received Timestamp(Nanosecond, None)"
        );
    }
}
