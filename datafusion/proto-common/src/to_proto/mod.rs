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

use crate::protobuf_common as protobuf;
use crate::protobuf_common::{
    arrow_type::ArrowTypeEnum, scalar_value::Value, EmptyMessage,
};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::csv::WriterBuilder;
use arrow::datatypes::{DataType, Field, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit, Schema, SchemaRef, TimeUnit, UnionMode};
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};
use datafusion_common::{
    config::{
        ColumnOptions, CsvOptions, JsonOptions, ParquetOptions, TableParquetOptions,
    },
    file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
    parsers::CompressionTypeVariant,
    plan_datafusion_err,
    stats::Precision,
    Column, ColumnStatistics, Constraint, Constraints, DFSchema, DFSchemaRef,
    DataFusionError, JoinSide, ScalarValue, Statistics,
};
use datafusion_common::logical_type::{ExtensionType, TypeRelation};

#[derive(Debug)]
pub enum Error {
    General(String),

    InvalidScalarValue(ScalarValue),

    InvalidScalarType(DataType),

    InvalidTimeUnit(TimeUnit),

    NotImplemented(String),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::General(desc) => write!(f, "General error: {desc}"),
            Self::InvalidScalarValue(value) => {
                write!(f, "{value:?} is invalid as a DataFusion scalar value")
            }
            Self::InvalidScalarType(data_type) => {
                write!(f, "{data_type:?} is invalid as a DataFusion scalar type")
            }
            Self::InvalidTimeUnit(time_unit) => {
                write!(
                    f,
                    "Only TimeUnit::Microsecond and TimeUnit::Nanosecond are valid time units, found: {time_unit:?}"
                )
            }
            Self::NotImplemented(s) => {
                write!(f, "Not implemented: {s}")
            }
        }
    }
}

impl From<Error> for DataFusionError {
    fn from(e: Error) -> Self {
        plan_datafusion_err!("{}", e)
    }
}

impl TryFrom<&Field> for protobuf::Field {
    type Error = Error;

    fn try_from(field: &Field) -> Result<Self, Self::Error> {
        let arrow_type = field.data_type().try_into()?;
        Ok(Self {
            name: field.name().to_owned(),
            arrow_type: Some(Box::new(arrow_type)),
            nullable: field.is_nullable(),
            children: Vec::new(),
            metadata: field.metadata().clone(),
            dict_id: field.dict_id().unwrap_or(0),
            dict_ordered: field.dict_is_ordered().unwrap_or(false),
        })
    }
}

impl TryFrom<&DataType> for protobuf::ArrowType {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let arrow_type_enum: ArrowTypeEnum = val.try_into()?;
        Ok(Self {
            arrow_type_enum: Some(arrow_type_enum),
        })
    }
}

impl TryFrom<&TypeRelation> for protobuf::ArrowType {
    type Error = Error;

    fn try_from(val: &TypeRelation) -> Result<Self, Self::Error> {
        let arrow_type_enum: ArrowTypeEnum = val.physical().try_into()?;
        Ok(Self {
            arrow_type_enum: Some(arrow_type_enum),
        })
    }
}

impl TryFrom<&DataType> for protobuf::arrow_type::ArrowTypeEnum {
    type Error = Error;

    fn try_from(val: &DataType) -> Result<Self, Self::Error> {
        let res = match val {
            DataType::Null => Self::None(EmptyMessage {}),
            DataType::Boolean => Self::Bool(EmptyMessage {}),
            DataType::Int8 => Self::Int8(EmptyMessage {}),
            DataType::Int16 => Self::Int16(EmptyMessage {}),
            DataType::Int32 => Self::Int32(EmptyMessage {}),
            DataType::Int64 => Self::Int64(EmptyMessage {}),
            DataType::UInt8 => Self::Uint8(EmptyMessage {}),
            DataType::UInt16 => Self::Uint16(EmptyMessage {}),
            DataType::UInt32 => Self::Uint32(EmptyMessage {}),
            DataType::UInt64 => Self::Uint64(EmptyMessage {}),
            DataType::Float16 => Self::Float16(EmptyMessage {}),
            DataType::Float32 => Self::Float32(EmptyMessage {}),
            DataType::Float64 => Self::Float64(EmptyMessage {}),
            DataType::Timestamp(time_unit, timezone) => {
                Self::Timestamp(protobuf::Timestamp {
                    time_unit: protobuf::TimeUnit::from(time_unit) as i32,
                    timezone: timezone.as_deref().unwrap_or("").to_string(),
                })
            }
            DataType::Date32 => Self::Date32(EmptyMessage {}),
            DataType::Date64 => Self::Date64(EmptyMessage {}),
            DataType::Time32(time_unit) => {
                Self::Time32(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Time64(time_unit) => {
                Self::Time64(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Duration(time_unit) => {
                Self::Duration(protobuf::TimeUnit::from(time_unit) as i32)
            }
            DataType::Interval(interval_unit) => {
                Self::Interval(protobuf::IntervalUnit::from(interval_unit) as i32)
            }
            DataType::Binary => Self::Binary(EmptyMessage {}),
            DataType::FixedSizeBinary(size) => Self::FixedSizeBinary(*size),
            DataType::LargeBinary => Self::LargeBinary(EmptyMessage {}),
            DataType::Utf8 => Self::Utf8(EmptyMessage {}),
            DataType::LargeUtf8 => Self::LargeUtf8(EmptyMessage {}),
            DataType::List(item_type) => Self::List(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::FixedSizeList(item_type, size) => {
                Self::FixedSizeList(Box::new(protobuf::FixedSizeList {
                    field_type: Some(Box::new(item_type.as_ref().try_into()?)),
                    list_size: *size,
                }))
            }
            DataType::LargeList(item_type) => Self::LargeList(Box::new(protobuf::List {
                field_type: Some(Box::new(item_type.as_ref().try_into()?)),
            })),
            DataType::Struct(struct_fields) => Self::Struct(protobuf::Struct {
                sub_field_types: convert_arc_fields_to_proto_fields(struct_fields)?,
            }),
            DataType::Union(fields, union_mode) => {
                let union_mode = match union_mode {
                    UnionMode::Sparse => protobuf::UnionMode::Sparse,
                    UnionMode::Dense => protobuf::UnionMode::Dense,
                };
                Self::Union(protobuf::Union {
                    union_types: convert_arc_fields_to_proto_fields(fields.iter().map(|(_, item)|item))?,
                    union_mode: union_mode.into(),
                    type_ids: fields.iter().map(|(x, _)| x as i32).collect(),
                })
            }
            DataType::Dictionary(key_type, value_type) => {
                Self::Dictionary(Box::new(protobuf::Dictionary {
                    key: Some(Box::new(key_type.as_ref().try_into()?)),
                    value: Some(Box::new(value_type.as_ref().try_into()?)),
                }))
            }
            DataType::Decimal128(precision, scale) => Self::Decimal(protobuf::Decimal {
                precision: *precision as u32,
                scale: *scale as i32,
            }),
            DataType::Decimal256(_, _) => {
                return Err(Error::General("Proto serialization error: The Decimal256 data type is not yet supported".to_owned()))
            }
            DataType::Map(field, sorted) => {
                Self::Map(Box::new(
                    protobuf::Map {
                        field_type: Some(Box::new(field.as_ref().try_into()?)),
                        keys_sorted: *sorted,
                    }
                ))
            }
            DataType::RunEndEncoded(_, _) => {
                return Err(Error::General(
                    "Proto serialization error: The RunEndEncoded data type is not yet supported".to_owned()
                ))
            }
            DataType::Utf8View | DataType::BinaryView | DataType::ListView(_) | DataType::LargeListView(_) => {
                return Err(Error::General(format!("Proto serialization error: {val} not yet supported")))
            }
        };

        Ok(res)
    }
}

impl From<Column> for protobuf::Column {
    fn from(c: Column) -> Self {
        Self {
            relation: c.relation.map(|relation| protobuf::ColumnRelation {
                relation: relation.to_string(),
            }),
            name: c.name,
        }
    }
}

impl From<&Column> for protobuf::Column {
    fn from(c: &Column) -> Self {
        c.clone().into()
    }
}

impl TryFrom<&Schema> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<SchemaRef> for protobuf::Schema {
    type Error = Error;

    fn try_from(schema: SchemaRef) -> Result<Self, Self::Error> {
        Ok(Self {
            columns: convert_arc_fields_to_proto_fields(schema.fields())?,
            metadata: schema.metadata.clone(),
        })
    }
}

impl TryFrom<&DFSchema> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchema) -> Result<Self, Self::Error> {
        let columns = s
            .iter()
            .map(|(qualifier, field)| {
                let field: Field = field.as_ref().clone().into();
                Ok(protobuf::DfField {
                    field: Some((&field).try_into()?),
                    qualifier: qualifier.map(|r| protobuf::ColumnRelation {
                        relation: r.to_string(),
                    }),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self {
            columns,
            metadata: s.metadata().clone(),
        })
    }
}

impl TryFrom<&DFSchemaRef> for protobuf::DfSchema {
    type Error = Error;

    fn try_from(s: &DFSchemaRef) -> Result<Self, Self::Error> {
        s.as_ref().try_into()
    }
}

impl TryFrom<&ScalarValue> for protobuf::ScalarValue {
    type Error = Error;

    fn try_from(val: &ScalarValue) -> Result<Self, Self::Error> {
        let data_type = val.data_type();
        match val {
            ScalarValue::Boolean(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::BoolValue(*s))
            }
            ScalarValue::Float16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Float32Value((*s).into())
                })
            }
            ScalarValue::Float32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float32Value(*s))
            }
            ScalarValue::Float64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Float64Value(*s))
            }
            ScalarValue::Int8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int8Value(*s as i32)
                })
            }
            ScalarValue::Int16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Int16Value(*s as i32)
                })
            }
            ScalarValue::Int32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int32Value(*s))
            }
            ScalarValue::Int64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Int64Value(*s))
            }
            ScalarValue::UInt8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint8Value(*s as u32)
                })
            }
            ScalarValue::UInt16(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Uint16Value(*s as u32)
                })
            }
            ScalarValue::UInt32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint32Value(*s))
            }
            ScalarValue::UInt64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Uint64Value(*s))
            }
            ScalarValue::Utf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Utf8Value(s.to_owned())
                })
            }
            ScalarValue::LargeUtf8(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeUtf8Value(s.to_owned())
                })
            }
            ScalarValue::Utf8View(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::Utf8ViewValue(s.to_owned())
                })
            }
            ScalarValue::List(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::LargeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::FixedSizeList(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Struct(arr) => {
                encode_scalar_nested_value(arr.to_owned() as ArrayRef, val)
            }
            ScalarValue::Date32(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date32Value(*s))
            }
            ScalarValue::TimestampMicrosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMicrosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::TimestampNanosecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeNanosecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::Decimal128(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal128Value(protobuf::Decimal128 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(protobuf::scalar_value::Value::NullValue(
                        (&data_type).try_into()?,
                    )),
                }),
            },
            ScalarValue::Decimal256(val, p, s) => match *val {
                Some(v) => {
                    let array = v.to_be_bytes();
                    let vec_val: Vec<u8> = array.to_vec();
                    Ok(protobuf::ScalarValue {
                        value: Some(Value::Decimal256Value(protobuf::Decimal256 {
                            value: vec_val,
                            p: *p as i64,
                            s: *s as i64,
                        })),
                    })
                }
                None => Ok(protobuf::ScalarValue {
                    value: Some(protobuf::scalar_value::Value::NullValue(
                        (&data_type).try_into()?,
                    )),
                }),
            },
            ScalarValue::Date64(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| Value::Date64Value(*s))
            }
            ScalarValue::TimestampSecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeSecondValue(*s),
                        ),
                    })
                })
            }
            ScalarValue::TimestampMillisecond(val, tz) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::TimestampValue(protobuf::ScalarTimestampValue {
                        timezone: tz.as_deref().unwrap_or("").to_string(),
                        value: Some(
                            protobuf::scalar_timestamp_value::Value::TimeMillisecondValue(
                                *s,
                            ),
                        ),
                    })
                })
            }
            ScalarValue::IntervalYearMonth(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::IntervalYearmonthValue(*s)
                })
            }
            ScalarValue::Null => Ok(protobuf::ScalarValue {
                value: Some(Value::NullValue((&data_type).try_into()?)),
            }),

            ScalarValue::Binary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::BinaryValue(s.to_owned())
                })
            }
            ScalarValue::BinaryView(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::BinaryViewValue(s.to_owned())
                })
            }
            ScalarValue::LargeBinary(val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::LargeBinaryValue(s.to_owned())
                })
            }
            ScalarValue::FixedSizeBinary(length, val) => {
                create_proto_scalar(val.as_ref(), &data_type, |s| {
                    Value::FixedSizeBinaryValue(protobuf::ScalarFixedSizeBinary {
                        values: s.to_owned(),
                        length: *length,
                    })
                })
            }

            ScalarValue::Time32Second(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32SecondValue(*v),
                        ),
                    })
                })
            }

            ScalarValue::Time32Millisecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time32Value(protobuf::ScalarTime32Value {
                        value: Some(
                            protobuf::scalar_time32_value::Value::Time32MillisecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Microsecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64MicrosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::Time64Nanosecond(v) => {
                create_proto_scalar(v.as_ref(), &data_type, |v| {
                    Value::Time64Value(protobuf::ScalarTime64Value {
                        value: Some(
                            protobuf::scalar_time64_value::Value::Time64NanosecondValue(
                                *v,
                            ),
                        ),
                    })
                })
            }

            ScalarValue::IntervalDayTime(val) => {
                let value = if let Some(v) = val {
                    let (days, milliseconds) = IntervalDayTimeType::to_parts(*v);
                    Value::IntervalDaytimeValue(protobuf::IntervalDayTimeValue {
                        days,
                        milliseconds,
                    })
                } else {
                    Value::NullValue((&data_type).try_into()?)
                };

                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::IntervalMonthDayNano(v) => {
                let value = if let Some(v) = v {
                    let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
                    Value::IntervalMonthDayNano(protobuf::IntervalMonthDayNanoValue {
                        months,
                        days,
                        nanos,
                    })
                } else {
                    Value::NullValue((&data_type).try_into()?)
                };

                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::DurationSecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationSecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMillisecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMillisecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationMicrosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationMicrosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }
            ScalarValue::DurationNanosecond(v) => {
                let value = match v {
                    Some(v) => Value::DurationNanosecondValue(*v),
                    None => Value::NullValue((&data_type).try_into()?),
                };
                Ok(protobuf::ScalarValue { value: Some(value) })
            }

            ScalarValue::Union(val, df_fields, mode) => {
                let mut fields =
                    Vec::<protobuf::UnionField>::with_capacity(df_fields.len());
                for (id, field) in df_fields.iter() {
                    let field_id = id as i32;
                    let field = Some(field.as_ref().try_into()?);
                    let field = protobuf::UnionField { field_id, field };
                    fields.push(field);
                }
                let mode = match mode {
                    UnionMode::Sparse => 0,
                    UnionMode::Dense => 1,
                };
                let value = match val {
                    None => None,
                    Some((_id, v)) => Some(Box::new(v.as_ref().try_into()?)),
                };
                let val = protobuf::UnionValue {
                    value_id: val.as_ref().map(|(id, _v)| *id as i32).unwrap_or(0),
                    value,
                    fields,
                    mode,
                };
                let val = Value::UnionValue(Box::new(val));
                let val = protobuf::ScalarValue { value: Some(val) };
                Ok(val)
            }

            ScalarValue::Dictionary(index_type, val) => {
                let value: protobuf::ScalarValue = val.as_ref().try_into()?;
                Ok(protobuf::ScalarValue {
                    value: Some(Value::DictionaryValue(Box::new(
                        protobuf::ScalarDictionaryValue {
                            index_type: Some(index_type.as_ref().try_into()?),
                            value: Some(Box::new(value)),
                        },
                    ))),
                })
            }
        }
    }
}

impl From<&TimeUnit> for protobuf::TimeUnit {
    fn from(val: &TimeUnit) -> Self {
        match val {
            TimeUnit::Second => protobuf::TimeUnit::Second,
            TimeUnit::Millisecond => protobuf::TimeUnit::Millisecond,
            TimeUnit::Microsecond => protobuf::TimeUnit::Microsecond,
            TimeUnit::Nanosecond => protobuf::TimeUnit::Nanosecond,
        }
    }
}

impl From<&IntervalUnit> for protobuf::IntervalUnit {
    fn from(interval_unit: &IntervalUnit) -> Self {
        match interval_unit {
            IntervalUnit::YearMonth => protobuf::IntervalUnit::YearMonth,
            IntervalUnit::DayTime => protobuf::IntervalUnit::DayTime,
            IntervalUnit::MonthDayNano => protobuf::IntervalUnit::MonthDayNano,
        }
    }
}

impl From<Constraints> for protobuf::Constraints {
    fn from(value: Constraints) -> Self {
        let constraints = value.into_iter().map(|item| item.into()).collect();
        protobuf::Constraints { constraints }
    }
}

impl From<Constraint> for protobuf::Constraint {
    fn from(value: Constraint) -> Self {
        let res = match value {
            Constraint::PrimaryKey(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
            Constraint::Unique(indices) => {
                let indices = indices.into_iter().map(|item| item as u64).collect();
                protobuf::constraint::ConstraintMode::PrimaryKey(
                    protobuf::PrimaryKeyConstraint { indices },
                )
            }
        };
        protobuf::Constraint {
            constraint_mode: Some(res),
        }
    }
}

impl From<&Precision<usize>> for protobuf::Precision {
    fn from(s: &Precision<usize>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: Some(crate::protobuf_common::ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: Some(crate::protobuf_common::ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(crate::protobuf_common::ScalarValue { value: None }),
            },
        }
    }
}

impl From<&Precision<datafusion_common::ScalarValue>> for protobuf::Precision {
    fn from(s: &Precision<datafusion_common::ScalarValue>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: val.try_into().ok(),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: val.try_into().ok(),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(crate::protobuf_common::ScalarValue { value: None }),
            },
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let column_stats = s.column_statistics.iter().map(|s| s.into()).collect();
        protobuf::Statistics {
            num_rows: Some(protobuf::Precision::from(&s.num_rows)),
            total_byte_size: Some(protobuf::Precision::from(&s.total_byte_size)),
            column_stats,
        }
    }
}

impl From<&ColumnStatistics> for protobuf::ColumnStats {
    fn from(s: &ColumnStatistics) -> protobuf::ColumnStats {
        protobuf::ColumnStats {
            min_value: Some(protobuf::Precision::from(&s.min_value)),
            max_value: Some(protobuf::Precision::from(&s.max_value)),
            null_count: Some(protobuf::Precision::from(&s.null_count)),
            distinct_count: Some(protobuf::Precision::from(&s.distinct_count)),
        }
    }
}

impl From<JoinSide> for protobuf::JoinSide {
    fn from(t: JoinSide) -> Self {
        match t {
            JoinSide::Left => protobuf::JoinSide::LeftSide,
            JoinSide::Right => protobuf::JoinSide::RightSide,
        }
    }
}

impl From<&CompressionTypeVariant> for protobuf::CompressionTypeVariant {
    fn from(value: &CompressionTypeVariant) -> Self {
        match value {
            CompressionTypeVariant::GZIP => Self::Gzip,
            CompressionTypeVariant::BZIP2 => Self::Bzip2,
            CompressionTypeVariant::XZ => Self::Xz,
            CompressionTypeVariant::ZSTD => Self::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => Self::Uncompressed,
        }
    }
}

impl TryFrom<&CsvWriterOptions> for protobuf::CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &CsvWriterOptions) -> datafusion_common::Result<Self, Self::Error> {
        Ok(csv_writer_options_to_proto(
            &opts.writer_options,
            &opts.compression,
        ))
    }
}

impl TryFrom<&JsonWriterOptions> for protobuf::JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(
        opts: &JsonWriterOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonWriterOptions {
            compression: compression.into(),
        })
    }
}

impl TryFrom<&ParquetOptions> for protobuf::ParquetOptions {
    type Error = DataFusionError;

    fn try_from(value: &ParquetOptions) -> datafusion_common::Result<Self, Self::Error> {
        Ok(protobuf::ParquetOptions {
            enable_page_index: value.enable_page_index,
            pruning: value.pruning,
            skip_metadata: value.skip_metadata,
            metadata_size_hint_opt: value.metadata_size_hint.map(|v| protobuf::parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v as u64)),
            pushdown_filters: value.pushdown_filters,
            reorder_filters: value.reorder_filters,
            data_pagesize_limit: value.data_pagesize_limit as u64,
            write_batch_size: value.write_batch_size as u64,
            writer_version: value.writer_version.clone(),
            compression_opt: value.compression.clone().map(protobuf::parquet_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value.dictionary_enabled.map(protobuf::parquet_options::DictionaryEnabledOpt::DictionaryEnabled),
            dictionary_page_size_limit: value.dictionary_page_size_limit as u64,
            statistics_enabled_opt: value.statistics_enabled.clone().map(protobuf::parquet_options::StatisticsEnabledOpt::StatisticsEnabled),
            max_statistics_size_opt: value.max_statistics_size.map(|v| protobuf::parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v as u64)),
            max_row_group_size: value.max_row_group_size as u64,
            created_by: value.created_by.clone(),
            column_index_truncate_length_opt: value.column_index_truncate_length.map(|v| protobuf::parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v as u64)),
            data_page_row_count_limit: value.data_page_row_count_limit as u64,
            encoding_opt: value.encoding.clone().map(protobuf::parquet_options::EncodingOpt::Encoding),
            bloom_filter_on_read: value.bloom_filter_on_read,
            bloom_filter_on_write: value.bloom_filter_on_write,
            bloom_filter_fpp_opt: value.bloom_filter_fpp.map(protobuf::parquet_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value.bloom_filter_ndv.map(protobuf::parquet_options::BloomFilterNdvOpt::BloomFilterNdv),
            allow_single_file_parallelism: value.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: value.maximum_parallel_row_group_writers as u64,
            maximum_buffered_record_batches_per_stream: value.maximum_buffered_record_batches_per_stream as u64,
        })
    }
}

impl TryFrom<&ColumnOptions> for protobuf::ColumnOptions {
    type Error = DataFusionError;

    fn try_from(value: &ColumnOptions) -> datafusion_common::Result<Self, Self::Error> {
        Ok(protobuf::ColumnOptions {
            compression_opt: value
                .compression
                .clone()
                .map(protobuf::column_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value
                .dictionary_enabled
                .map(protobuf::column_options::DictionaryEnabledOpt::DictionaryEnabled),
            statistics_enabled_opt: value
                .statistics_enabled
                .clone()
                .map(protobuf::column_options::StatisticsEnabledOpt::StatisticsEnabled),
            max_statistics_size_opt: value.max_statistics_size.map(|v| {
                protobuf::column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(
                    v as u32,
                )
            }),
            encoding_opt: value
                .encoding
                .clone()
                .map(protobuf::column_options::EncodingOpt::Encoding),
            bloom_filter_enabled_opt: value
                .bloom_filter_enabled
                .map(protobuf::column_options::BloomFilterEnabledOpt::BloomFilterEnabled),
            bloom_filter_fpp_opt: value
                .bloom_filter_fpp
                .map(protobuf::column_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value
                .bloom_filter_ndv
                .map(protobuf::column_options::BloomFilterNdvOpt::BloomFilterNdv),
        })
    }
}

impl TryFrom<&TableParquetOptions> for protobuf::TableParquetOptions {
    type Error = DataFusionError;
    fn try_from(
        value: &TableParquetOptions,
    ) -> datafusion_common::Result<Self, Self::Error> {
        let column_specific_options = value
            .column_specific_options
            .iter()
            .map(|(k, v)| {
                Ok(protobuf::ColumnSpecificOptions {
                    column_name: k.into(),
                    options: Some(v.try_into()?),
                })
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        Ok(protobuf::TableParquetOptions {
            global: Some((&value.global).try_into()?),
            column_specific_options,
        })
    }
}

impl TryFrom<&CsvOptions> for protobuf::CsvOptions {
    type Error = DataFusionError; // Define or use an appropriate error type

    fn try_from(opts: &CsvOptions) -> datafusion_common::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::CsvOptions {
            has_header: opts.has_header.map_or_else(Vec::new, |h| vec![h as u8]),
            delimiter: vec![opts.delimiter],
            quote: vec![opts.quote],
            escape: opts.escape.map_or_else(Vec::new, |e| vec![e]),
            double_quote: opts.double_quote.map_or_else(Vec::new, |h| vec![h as u8]),
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec as u64,
            date_format: opts.date_format.clone().unwrap_or_default(),
            datetime_format: opts.datetime_format.clone().unwrap_or_default(),
            timestamp_format: opts.timestamp_format.clone().unwrap_or_default(),
            timestamp_tz_format: opts.timestamp_tz_format.clone().unwrap_or_default(),
            time_format: opts.time_format.clone().unwrap_or_default(),
            null_value: opts.null_value.clone().unwrap_or_default(),
            comment: opts.comment.map_or_else(Vec::new, |h| vec![h]),
        })
    }
}

impl TryFrom<&JsonOptions> for protobuf::JsonOptions {
    type Error = DataFusionError;

    fn try_from(opts: &JsonOptions) -> datafusion_common::Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonOptions {
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec as u64,
        })
    }
}

/// Creates a scalar protobuf value from an optional value (T), and
/// encoding None as the appropriate datatype
fn create_proto_scalar<I, T: FnOnce(&I) -> protobuf::scalar_value::Value>(
    v: Option<&I>,
    null_arrow_type: &DataType,
    constructor: T,
) -> Result<protobuf::ScalarValue, Error> {
    let value = v
        .map(constructor)
        .unwrap_or(protobuf::scalar_value::Value::NullValue(
            null_arrow_type.try_into()?,
        ));

    Ok(protobuf::ScalarValue { value: Some(value) })
}

// ScalarValue::List / FixedSizeList / LargeList / Struct are serialized using
// Arrow IPC messages as a single column RecordBatch
fn encode_scalar_nested_value(
    arr: ArrayRef,
    val: &ScalarValue,
) -> Result<protobuf::ScalarValue, Error> {
    let batch = RecordBatch::try_from_iter(vec![("field_name", arr)]).map_err(|e| {
        Error::General(format!(
            "Error creating temporary batch while encoding ScalarValue::List: {e}"
        ))
    })?;

    let gen = IpcDataGenerator {};
    let mut dict_tracker = DictionaryTracker::new(false);
    let (encoded_dictionaries, encoded_message) = gen
        .encoded_batch(&batch, &mut dict_tracker, &Default::default())
        .map_err(|e| {
            Error::General(format!("Error encoding ScalarValue::List as IPC: {e}"))
        })?;

    let schema: protobuf::Schema = batch.schema().try_into()?;

    let scalar_list_value = protobuf::ScalarNestedValue {
        ipc_message: encoded_message.ipc_message,
        arrow_data: encoded_message.arrow_data,
        dictionaries: encoded_dictionaries
            .into_iter()
            .map(|data| protobuf::scalar_nested_value::Dictionary {
                ipc_message: data.ipc_message,
                arrow_data: data.arrow_data,
            })
            .collect(),
        schema: Some(schema),
    };

    match val {
        ScalarValue::List(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::ListValue(scalar_list_value)),
        }),
        ScalarValue::LargeList(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::LargeListValue(
                scalar_list_value,
            )),
        }),
        ScalarValue::FixedSizeList(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::FixedSizeListValue(
                scalar_list_value,
            )),
        }),
        ScalarValue::Struct(_) => Ok(protobuf::ScalarValue {
            value: Some(protobuf::scalar_value::Value::StructValue(
                scalar_list_value,
            )),
        }),
        _ => unreachable!(),
    }
}

/// Converts a vector of `Arc<arrow::Field>`s to `protobuf::Field`s
fn convert_arc_fields_to_proto_fields<'a, I>(
    fields: I,
) -> Result<Vec<protobuf::Field>, Error>
where
    I: IntoIterator<Item = &'a Arc<Field>>,
{
    fields
        .into_iter()
        .map(|field| field.as_ref().try_into())
        .collect::<Result<Vec<_>, Error>>()
}

pub(crate) fn csv_writer_options_to_proto(
    csv_options: &WriterBuilder,
    compression: &CompressionTypeVariant,
) -> protobuf::CsvWriterOptions {
    let compression: protobuf::CompressionTypeVariant = compression.into();
    protobuf::CsvWriterOptions {
        compression: compression.into(),
        delimiter: (csv_options.delimiter() as char).to_string(),
        has_header: csv_options.header(),
        date_format: csv_options.date_format().unwrap_or("").to_owned(),
        datetime_format: csv_options.datetime_format().unwrap_or("").to_owned(),
        timestamp_format: csv_options.timestamp_format().unwrap_or("").to_owned(),
        time_format: csv_options.time_format().unwrap_or("").to_owned(),
        null_value: csv_options.null().to_owned(),
        quote: (csv_options.quote() as char).to_string(),
        escape: (csv_options.escape() as char).to_string(),
        double_quote: csv_options.double_quote(),
    }
}
