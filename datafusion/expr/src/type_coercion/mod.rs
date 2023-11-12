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

//! Type coercion rules for DataFusion
//!
//! Coercion is performed automatically by DataFusion when the types
//! of arguments passed to a function or needed by operators do not
//! exacty match the types required by that function / operator. In
//! this case, DataFusion will attempt to *coerce* the arguments to
//! types accepted by the function by inserting CAST operations.
//!
//! CAST operations added by coercion are lossless and never discard
//! information.
//!
//! For example coercion from i32 -> i64 might be
//! performed because all valid i32 values can be represented using an
//! i64. However, i64 -> i32 is never performed as there are i64
//! values which can not be represented by i32 values.

pub mod aggregates;
pub mod binary;
pub mod functions;
pub mod other;

use arrow::datatypes::DataType;
use datafusion_common::logical_type::LogicalType;
/// Determine whether the given data type `dt` represents signed numeric values.
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _),
    )
}

/// Determine whether the given data type `dt` is `Null`.
pub fn is_null(dt: &DataType) -> bool {
    *dt == DataType::Null
}

/// Determine whether the given data type `dt` is a `Timestamp`.
pub fn is_timestamp(dt: &DataType) -> bool {
    matches!(dt, DataType::Timestamp(_, _))
}

/// Determine whether the given data type 'dt' is a `Interval`.
pub fn is_interval(dt: &DataType) -> bool {
    matches!(dt, DataType::Interval(_))
}

/// Determine whether the given data type `dt` is a `Date` or `Timestamp`.
pub fn is_datetime(dt: &LogicalType) -> bool {
    matches!(
        dt,
        LogicalType::Date32 | LogicalType::Date64 | LogicalType::Timestamp(_, _)
    )
}

/// Determine whether the given data type `dt` is a `Utf8` or `LargeUtf8`.
pub fn is_utf8_or_large_utf8(dt: &LogicalType) -> bool {
    matches!(dt, LogicalType::Utf8 | LogicalType::LargeUtf8)
}

/// Determine whether the given data type `dt` is a `Decimal`.
pub fn is_decimal(dt: &LogicalType) -> bool {
    matches!(
        dt,
        LogicalType::Decimal128(_, _) | LogicalType::Decimal256(_, _)
    )
}
