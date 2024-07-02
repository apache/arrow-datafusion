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

use std::{fmt::Display, sync::Arc};

use arrow_schema::DataType;

use field::{LogicalField, LogicalFieldRef};
use fields::LogicalFields;
use signature::LogicalType;

pub mod field;
pub mod fields;
pub mod schema;
pub mod signature;

pub type ExtensionTypeRef = Arc<dyn ExtensionType + Send + Sync>;

pub trait ExtensionType: std::fmt::Debug {
    fn logical(&self) -> &LogicalType;
    fn physical(&self) -> &DataType;
}

#[derive(Clone, Debug)]
pub struct TypeRelation(ExtensionTypeRef);

impl TypeRelation {
    pub fn new_list(inner: TypeRelation, nullable: bool) -> Self {
        Self(Arc::new(NativeType::new_list(inner, nullable)))
    }

    pub fn new_struct(fields: LogicalFields) -> Self {
        Self(Arc::new(NativeType::new_struct(fields)))
    }
}

pub type NativeTypeRef = Arc<NativeType>;

#[derive(Clone, Debug)]
pub struct NativeType {
    logical: LogicalType,
    physical: DataType,
}

impl ExtensionType for NativeType {
    fn logical(&self) -> &LogicalType {
        &self.logical
    }

    fn physical(&self) -> &DataType {
        &self.physical
    }
}

impl NativeType {
    pub fn new_list(inner: TypeRelation, nullable: bool) -> Self {
        Self {
            physical: DataType::new_list(inner.physical().clone(), nullable),
            logical: LogicalType::List(LogicalFieldRef::new(LogicalField::new_list_field(inner, nullable))),
        }
    }

    pub fn new_struct(fields: LogicalFields) -> Self {
        Self {
            physical: DataType::Struct(fields.clone().into()),
            logical: LogicalType::Struct(fields),
        }
    }
}

impl ExtensionType for TypeRelation {
    fn logical(&self) -> &LogicalType {
        self.0.logical()
    }

    fn physical(&self) -> &DataType {
        self.0.physical()
    }
}

impl PartialEq for TypeRelation {
    fn eq(&self, other: &Self) -> bool {
        self.logical() == other.logical()
    }
}

impl Eq for TypeRelation {}

impl std::hash::Hash for TypeRelation {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.logical().hash(state)
    }
}

impl Display for TypeRelation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<DataType> for NativeType {
    fn from(value: DataType) -> Self {
        Self {
            logical: (&value).into(),
            physical: value,
        }
    }
}

impl From<ExtensionTypeRef> for TypeRelation {
    fn from(value: ExtensionTypeRef) -> Self {
        Self(value)
    }
}

impl From<&DataType> for TypeRelation {
    fn from(value: &DataType) -> Self {
        value.clone().into()
    }
}

impl From<DataType> for TypeRelation {
    fn from(value: DataType) -> Self {
        Self(NativeTypeRef::new(value.into()))
    }
}

impl From<DataType> for LogicalType {
    fn from(value: DataType) -> Self {
        (&value).into()
    }
}

impl From<&DataType> for LogicalType {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Null => LogicalType::Null,
            DataType::Boolean => LogicalType::Boolean,
            DataType::Int8 => LogicalType::Int8,
            DataType::Int16 => LogicalType::Int16,
            DataType::Int32 => LogicalType::Int32,
            DataType::Int64 => LogicalType::Int64,
            DataType::UInt8 => LogicalType::UInt8,
            DataType::UInt16 => LogicalType::UInt16,
            DataType::UInt32 => LogicalType::UInt32,
            DataType::UInt64 => LogicalType::UInt64,
            DataType::Float16 => LogicalType::Float16,
            DataType::Float32 => LogicalType::Float32,
            DataType::Float64 => LogicalType::Float64,
            DataType::Timestamp(tu, tz) => {
                LogicalType::Timestamp(tu.clone(), tz.clone())
            }
            DataType::Date32 | DataType::Date64 => LogicalType::Date,
            DataType::Time32(tu) => LogicalType::Time32(tu.clone()),
            DataType::Time64(tu) => LogicalType::Time64(tu.clone()),
            DataType::Duration(tu) => LogicalType::Duration(tu.clone()),
            DataType::Interval(iu) => LogicalType::Interval(iu.clone()),
            DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView => LogicalType::Binary,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                LogicalType::Utf8
            }
            DataType::List(f)
            | DataType::ListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::LargeList(f)
            | DataType::LargeListView(f) => {
                LogicalType::List(LogicalFieldRef::new(f.as_ref().clone().into()))
            }
            DataType::Struct(f) => LogicalType::Struct(f.clone().into()),
            DataType::Dictionary(_, t) => t.as_ref().into(),
            DataType::Decimal128(precision, scale) => {
                LogicalType::Decimal128(precision.clone(), scale.clone())
            }
            DataType::Decimal256(precision, scale) => {
                LogicalType::Decimal256(precision.clone(), scale.clone())
            }
            DataType::Map(f, sorted) => LogicalType::Map(
                LogicalFieldRef::new(f.as_ref().clone().into()),
                sorted.clone(),
            ),
            DataType::RunEndEncoded(_, f) => f.data_type().into(),
            DataType::Union(f, _) => LogicalType::Union(f.into()),
        }
    }
}
