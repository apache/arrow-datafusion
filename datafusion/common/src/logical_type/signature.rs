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

use core::fmt;
use std::sync::Arc;

use arrow_schema::{IntervalUnit, TimeUnit};

use super::{field::LogicalFieldRef, fields::{LogicalFields, LogicalUnionFields}};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalType {
    Null,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Boolean,
    Float16,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit, Option<Arc<str>>),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    List(LogicalFieldRef),
    Struct(LogicalFields),
    Map(LogicalFieldRef, bool),
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Union(LogicalUnionFields)
    // TODO: extension signatures?
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl LogicalType {
    /// Returns true if the type is primitive: (numeric, temporal).
    #[inline]
    pub fn is_primitive(&self) -> bool {
        self.is_numeric() || self.is_temporal()
    }

    /// Returns true if this type is numeric: (UInt*, Int*, Float*, Decimal*).
    #[inline]
    pub fn is_numeric(&self) -> bool {
        use LogicalType::*;
        matches!(
            self,
            UInt8
                | UInt16
                | UInt32
                | UInt64
                | Int8
                | Int16
                | Int32
                | Int64
                | Float16
                | Float32
                | Float64
                | Decimal128(_, _)
                | Decimal256(_, _)
        )
    }

    /// Returns true if this type is temporal: (Date*, Time*, Duration, or Interval).
    #[inline]
    pub fn is_temporal(&self) -> bool {
        use LogicalType::*;
        matches!(
            self,
            Date | Timestamp(_, _) | Time32(_) | Time64(_) | Duration(_) | Interval(_)
        )
    }

    /// Returns true if this type is floating: (Float*).
    #[inline]
    pub fn is_floating(&self) -> bool {
        use LogicalType::*;
        matches!(self, Float16 | Float32 | Float64)
    }

    /// Returns true if this type is integer: (Int*, UInt*).
    #[inline]
    pub fn is_integer(&self) -> bool {
        self.is_signed_integer() || self.is_unsigned_integer()
    }

    /// Returns true if this type is signed integer: (Int*).
    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        use LogicalType::*;
        matches!(self, Int8 | Int16 | Int32 | Int64)
    }

    /// Returns true if this type is unsigned integer: (UInt*).
    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        use LogicalType::*;
        matches!(self, UInt8 | UInt16 | UInt32 | UInt64)
    }

    /// Returns true if this type is TypeSignature::Null.
    #[inline]
    pub fn is_null(&self) -> bool {
        use LogicalType::*;
        matches!(self, Null)
    }
}
