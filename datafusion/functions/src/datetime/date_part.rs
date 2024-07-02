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

use arrow::array::{Array, ArrayRef, Float64Array};
use arrow::compute::{binary, cast, date_part, DatePart};
use arrow::datatypes::DataType::{
    Date32, Date64, Float64, Time32, Time64, Timestamp, Utf8,
};
use arrow::datatypes::TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
use arrow::datatypes::{DataType, TimeUnit};

use datafusion_common::cast::{
    as_date32_array, as_date64_array, as_int32_array, as_time32_millisecond_array,
    as_time32_second_array, as_time64_microsecond_array, as_time64_nanosecond_array,
    as_timestamp_microsecond_array, as_timestamp_millisecond_array,
    as_timestamp_nanosecond_array, as_timestamp_second_array,
};
use datafusion_common::{exec_err, Result, ScalarValue};
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, Volatility, TIMEZONE_WILDCARD,
};

#[derive(Debug)]
pub struct DatePartFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for DatePartFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl DatePartFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Timestamp(Nanosecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Nanosecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Millisecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Millisecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Microsecond, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Microsecond, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Timestamp(Second, None)]),
                    Exact(vec![
                        Utf8,
                        Timestamp(Second, Some(TIMEZONE_WILDCARD.into())),
                    ]),
                    Exact(vec![Utf8, Date64]),
                    Exact(vec![Utf8, Date32]),
                    Exact(vec![Utf8, Time32(Second)]),
                    Exact(vec![Utf8, Time32(Millisecond)]),
                    Exact(vec![Utf8, Time64(Microsecond)]),
                    Exact(vec![Utf8, Time64(Nanosecond)]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

impl ScalarUDFImpl for DatePartFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "date_part"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() != 2 {
            return exec_err!("Expected two arguments in DATE_PART");
        }
        let (part, array) = (&args[0], &args[1]);

        let part = if let ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) = part {
            v
        } else {
            return exec_err!(
                "First argument of `DATE_PART` must be non-null scalar Utf8"
            );
        };

        let is_scalar = matches!(array, ColumnarValue::Scalar(_));

        let array = match array {
            ColumnarValue::Array(array) => Arc::clone(array),
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };

        // to remove quotes at most 2 characters
        let part_trim = part.trim_matches(|c| c == '\'' || c == '\"');
        if ![2, 0].contains(&(part.len() - part_trim.len())) {
            return exec_err!("Date part '{part}' not supported");
        }

        let arr = match part_trim.to_lowercase().as_str() {
            "year" => date_part_f64(array.as_ref(), DatePart::Year)?,
            "quarter" => date_part_f64(array.as_ref(), DatePart::Quarter)?,
            "month" => date_part_f64(array.as_ref(), DatePart::Month)?,
            "week" => date_part_f64(array.as_ref(), DatePart::Week)?,
            "day" => date_part_f64(array.as_ref(), DatePart::Day)?,
            "doy" => date_part_f64(array.as_ref(), DatePart::DayOfYear)?,
            "dow" => date_part_f64(array.as_ref(), DatePart::DayOfWeekSunday0)?,
            "hour" => date_part_f64(array.as_ref(), DatePart::Hour)?,
            "minute" => date_part_f64(array.as_ref(), DatePart::Minute)?,
            "second" => seconds(array.as_ref(), Second)?,
            "millisecond" => seconds(array.as_ref(), Millisecond)?,
            "microsecond" => seconds(array.as_ref(), Microsecond)?,
            "nanosecond" => seconds(array.as_ref(), Nanosecond)?,
            "epoch" => epoch(array.as_ref())?,
            _ => return exec_err!("Date part '{part}' not supported"),
        };

        Ok(if is_scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(arr.as_ref(), 0)?)
        } else {
            ColumnarValue::Array(arr)
        })
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

/// Invoke [`date_part`] and cast the result to Float64
fn date_part_f64(array: &dyn Array, part: DatePart) -> Result<ArrayRef> {
    Ok(cast(date_part(array, part)?.as_ref(), &Float64)?)
}

/// Invoke [`date_part`] on an `array` (e.g. Timestamp) and convert the
/// result to a total number of seconds, milliseconds, microseconds or
/// nanoseconds
fn seconds(array: &dyn Array, unit: TimeUnit) -> Result<ArrayRef> {
    let sf = match unit {
        Second => 1_f64,
        Millisecond => 1_000_f64,
        Microsecond => 1_000_000_f64,
        Nanosecond => 1_000_000_000_f64,
    };
    let secs = date_part(array, DatePart::Second)?;
    // This assumes array is primitive and not a dictionary
    let secs = as_int32_array(secs.as_ref())?;
    let subsecs = date_part(array, DatePart::Nanosecond)?;
    let subsecs = as_int32_array(subsecs.as_ref())?;

    let r: Float64Array = binary(secs, subsecs, |secs, subsecs| {
        (secs as f64 + (subsecs as f64 / 1_000_000_000_f64)) * sf
    })?;
    Ok(Arc::new(r))
}

fn epoch(array: &dyn Array) -> Result<ArrayRef> {
    const SECONDS_IN_A_DAY: f64 = 86400_f64;

    let f: Float64Array = match array.data_type() {
        Timestamp(Second, _) => as_timestamp_second_array(array)?.unary(|x| x as f64),
        Timestamp(Millisecond, _) => {
            as_timestamp_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Timestamp(Microsecond, _) => {
            as_timestamp_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Timestamp(Nanosecond, _) => {
            as_timestamp_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        Date32 => as_date32_array(array)?.unary(|x| x as f64 * SECONDS_IN_A_DAY),
        Date64 => as_date64_array(array)?.unary(|x| x as f64 / 1_000_f64),
        Time32(Second) => as_time32_second_array(array)?.unary(|x| x as f64),
        Time32(Millisecond) => {
            as_time32_millisecond_array(array)?.unary(|x| x as f64 / 1_000_f64)
        }
        Time64(Microsecond) => {
            as_time64_microsecond_array(array)?.unary(|x| x as f64 / 1_000_000_f64)
        }
        Time64(Nanosecond) => {
            as_time64_nanosecond_array(array)?.unary(|x| x as f64 / 1_000_000_000_f64)
        }
        d => return exec_err!("Can not convert {d:?} to epoch"),
    };
    Ok(Arc::new(f))
}
