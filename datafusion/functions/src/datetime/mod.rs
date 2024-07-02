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

//! date & time DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

pub mod common;
pub mod current_date;
pub mod current_time;
pub mod date_bin;
pub mod date_part;
pub mod date_trunc;
pub mod from_unixtime;
pub mod make_date;
pub mod now;
pub mod planner;
pub mod to_char;
pub mod to_date;
pub mod to_timestamp;
pub mod to_unixtime;

// create UDFs
make_udf_function!(current_date::CurrentDateFunc, CURRENT_DATE, current_date);
make_udf_function!(current_time::CurrentTimeFunc, CURRENT_TIME, current_time);
make_udf_function!(date_bin::DateBinFunc, DATE_BIN, date_bin);
make_udf_function!(date_part::DatePartFunc, DATE_PART, date_part);
make_udf_function!(date_trunc::DateTruncFunc, DATE_TRUNC, date_trunc);
make_udf_function!(make_date::MakeDateFunc, MAKE_DATE, make_date);
make_udf_function!(
    from_unixtime::FromUnixtimeFunc,
    FROM_UNIXTIME,
    from_unixtime
);
make_udf_function!(now::NowFunc, NOW, now);
make_udf_function!(to_char::ToCharFunc, TO_CHAR, to_char);
make_udf_function!(to_date::ToDateFunc, TO_DATE, to_date);
make_udf_function!(to_unixtime::ToUnixtimeFunc, TO_UNIXTIME, to_unixtime);
make_udf_function!(to_timestamp::ToTimestampFunc, TO_TIMESTAMP, to_timestamp);
make_udf_function!(
    to_timestamp::ToTimestampSecondsFunc,
    TO_TIMESTAMP_SECONDS,
    to_timestamp_seconds
);
make_udf_function!(
    to_timestamp::ToTimestampMillisFunc,
    TO_TIMESTAMP_MILLIS,
    to_timestamp_millis
);
make_udf_function!(
    to_timestamp::ToTimestampMicrosFunc,
    TO_TIMESTAMP_MICROS,
    to_timestamp_micros
);
make_udf_function!(
    to_timestamp::ToTimestampNanosFunc,
    TO_TIMESTAMP_NANOS,
    to_timestamp_nanos
);

// we cannot currently use the export_functions macro since it doesn't handle
// functions with varargs currently

pub mod expr_fn {
    use datafusion_expr::Expr;

    export_functions!((
        current_date,
        "returns current UTC date as a Date32 value",
    ),(
        current_time,
        "returns current UTC time as a Time64 value",
    ),(
        from_unixtime,
        "converts an integer to RFC3339 timestamp format string",
        unixtime
    ),(
        date_bin,
        "coerces an arbitrary timestamp to the start of the nearest specified interval",
        stride source origin
    ),(
        date_part,
        "extracts a subfield from the date",
        part date
    ),(
        date_trunc,
        "truncates the date to a specified level of precision",
        part date
    ),(
        make_date,
        "make a date from year, month and day component parts",
        year month day
    ),(
        now,
        "returns the current timestamp in nanoseconds, using the same value for all instances of now() in same statement",
    ),(
        to_unixtime,
        "converts a string and optional formats to a Unixtime",
        args,
    ),(
        to_timestamp,
        "converts a string and optional formats to a `Timestamp(Nanoseconds, None)`",
        args,
    ),(
        to_timestamp_seconds,
        "converts a string and optional formats to a `Timestamp(Seconds, None)`",
        args,
    ),(
        to_timestamp_millis,
        "converts a string and optional formats to a `Timestamp(Milliseconds, None)`",
        args,
    ),(
        to_timestamp_micros,
        "converts a string and optional formats to a `Timestamp(Microseconds, None)`",
        args,
    ),(
        to_timestamp_nanos,
        "converts a string and optional formats to a `Timestamp(Nanoseconds, None)`",
        args,
    ));

    /// Returns a string representation of a date, time, timestamp or duration based
    /// on a Chrono pattern.
    ///
    /// The syntax for the patterns can be found at
    /// <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>
    ///
    /// # Examples
    ///
    /// ```ignore
    /// # use chrono::prelude::*;
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use datafusion_common::ScalarValue::TimestampNanosecond;
    /// # use std::sync::Arc;
    /// # use arrow_array::{Date32Array, RecordBatch, StringArray};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("values", DataType::Date32, false),
    ///     Field::new("patterns", DataType::Utf8, false),
    /// ]));
    ///
    /// let batch = RecordBatch::try_new(
    ///     schema,
    ///     vec![
    ///         Arc::new(Date32Array::from(vec![
    ///             18506,
    ///             18507,
    ///             18508,
    ///             18509,
    ///         ])),
    ///         Arc::new(StringArray::from(vec![
    ///             "%Y-%m-%d",
    ///             "%Y:%m:%d",
    ///             "%Y%m%d",
    ///             "%d-%m-%Y",
    ///         ])),
    ///     ],
    /// )?;
    ///
    /// let ctx = SessionContext::new();
    /// ctx.register_batch("t", batch)?;
    /// let df = ctx.table("t").await?;
    ///
    /// // use the to_char function to convert col 'values',
    /// // to strings using patterns in col 'patterns'
    /// let df = df.with_column(
    ///     "date_str",
    ///     to_char(col("values"), col("patterns"))
    /// )?;
    /// // Note that providing a scalar value for the pattern
    /// // is more performant
    /// let df = df.with_column(
    ///     "date_str2",
    ///     to_char(col("values"), lit("%d-%m-%Y"))
    /// )?;
    /// // literals can be used as well with dataframe calls
    /// let timestamp = "2026-07-08T09:10:11"
    ///     .parse::<NaiveDateTime>()
    ///     .unwrap()
    ///     .with_nanosecond(56789)
    ///     .unwrap()
    ///     .timestamp_nanos_opt()
    ///     .unwrap();
    /// let df = df.with_column(
    ///     "timestamp_str",
    ///     to_char(lit(TimestampNanosecond(Some(timestamp), None)), lit("%d-%m-%Y %H:%M:%S"))
    /// )?;
    ///
    /// df.show().await?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn to_char(datetime: Expr, format: Expr) -> Expr {
        super::to_char().call(vec![datetime, format])
    }

    /// ```ignore
    /// # use std::sync::Arc;
    ///
    /// # use datafusion_common::Result;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// #  use arrow::array::StringArray;
    /// #  use arrow::datatypes::{DataType, Field, Schema};
    /// #  use arrow::record_batch::RecordBatch;
    /// #  use datafusion_expr::col;
    /// #  use datafusion::prelude::*;
    /// #  use datafusion_functions::expr_fn::to_date;
    ///
    ///     // define a schema.
    ///     let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));
    ///
    ///     // define data.
    ///     let batch = RecordBatch::try_new(
    ///         schema,
    ///         vec![Arc::new(StringArray::from(vec![
    ///             "2020-09-08T13:42:29Z",
    ///             "2020-09-08T13:42:29.190855-05:00",
    ///             "2020-08-09 12:13:29",
    ///             "2020-01-02",
    ///         ]))],
    ///     )?;
    ///
    ///     // declare a new context. In spark API, this corresponds to a new spark SQLsession
    ///     let ctx = SessionContext::new();
    ///
    ///     // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ///     ctx.register_batch("t", batch)?;
    ///     let df = ctx.table("t").await?;
    ///
    ///     // use to_date function to convert col 'a' to timestamp type using the default parsing
    ///     let df = df.with_column("a", to_date(vec![col("a")]))?;
    ///
    ///     let df = df.select_columns(&["a"])?;
    ///
    ///     // print the results
    ///     df.show().await?;
    ///
    ///     # Ok(())
    /// # }
    /// ```
    pub fn to_date(args: Vec<Expr>) -> Expr {
        super::to_date().call(args)
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        current_date(),
        current_time(),
        date_bin(),
        date_part(),
        date_trunc(),
        from_unixtime(),
        make_date(),
        now(),
        to_char(),
        to_date(),
        to_unixtime(),
        to_timestamp(),
        to_timestamp_seconds(),
        to_timestamp_millis(),
        to_timestamp_micros(),
        to_timestamp_nanos(),
    ]
}
