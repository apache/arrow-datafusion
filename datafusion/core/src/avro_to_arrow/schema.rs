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

use crate::arrow::datatypes::{DataType, IntervalUnit, Schema, TimeUnit, UnionMode};
use crate::error::{DataFusionError, Result};
use arrow::datatypes::Field;
use avro_rs::schema::Name;
use avro_rs::types::Value;
use avro_rs::Schema as AvroSchema;
use std::collections::BTreeMap;
use std::convert::TryFrom;

/// Converts an avro schema to an arrow schema
pub fn to_arrow_schema(avro_schema: &avro_rs::Schema) -> Result<Schema> {
    let mut schema_fields = vec![];
    match avro_schema {
        AvroSchema::Record { fields, .. } => {
            for field in fields {
                schema_fields.push(schema_to_field_with_props(
                    &field.schema,
                    Some(&field.name),
                    false,
                    Some(&external_props(&field.schema)),
                )?)
            }
        }
        schema => schema_fields.push(schema_to_field(schema, Some(""), false)?),
    }

    let schema = Schema::new(schema_fields);
    Ok(schema)
}

fn schema_to_field(
    schema: &avro_rs::Schema,
    name: Option<&str>,
    nullable: bool,
) -> Result<Field> {
    schema_to_field_with_props(schema, name, nullable, None)
}

fn schema_to_field_with_props(
    schema: &AvroSchema,
    name: Option<&str>,
    nullable: bool,
    props: Option<&BTreeMap<String, String>>,
) -> Result<Field> {
    let mut nullable = nullable;
    let field_type: DataType = match schema {
        AvroSchema::Null => DataType::Null,
        AvroSchema::Boolean => DataType::Boolean,
        AvroSchema::Int => DataType::Int32,
        AvroSchema::Long => DataType::Int64,
        AvroSchema::Float => DataType::Float32,
        AvroSchema::Double => DataType::Float64,
        AvroSchema::Bytes => DataType::Binary,
        AvroSchema::String => DataType::Utf8,
        AvroSchema::Array(item_schema) => DataType::List(Box::new(
            schema_to_field_with_props(item_schema, None, false, None)?,
        )),
        AvroSchema::Map(value_schema) => {
            let value_field =
                schema_to_field_with_props(value_schema, Some("value"), false, None)?;
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(value_field.data_type().clone()),
            )
        }
        AvroSchema::Union(us) => {
            // If there are only two variants and one of them is null, set the other type as the field data type
            let has_nullable = us.find_schema(&Value::Null).is_some();
            let sub_schemas = us.variants();
            if has_nullable && sub_schemas.len() == 2 {
                nullable = true;
                if let Some(schema) = sub_schemas
                    .iter()
                    .find(|&schema| !matches!(schema, AvroSchema::Null))
                {
                    schema_to_field_with_props(schema, None, has_nullable, None)?
                        .data_type()
                        .clone()
                } else {
                    return Err(DataFusionError::AvroError(
                        avro_rs::Error::GetUnionDuplicate,
                    ));
                }
            } else {
                let fields = sub_schemas
                    .iter()
                    .map(|s| schema_to_field_with_props(s, None, has_nullable, None))
                    .collect::<Result<Vec<Field>>>()?;
                let type_ids = (0_i8..fields.len() as i8).collect();
                DataType::Union(fields, type_ids, UnionMode::Dense)
            }
        }
        AvroSchema::Record { name, fields, .. } => {
            let fields: Result<Vec<Field>> = fields
                .iter()
                .map(|field| {
                    let mut props = BTreeMap::new();
                    if let Some(doc) = &field.doc {
                        props.insert("avro::doc".to_string(), doc.clone());
                    }
                    /*if let Some(aliases) = fields.aliases {
                        props.insert("aliases", aliases);
                    }*/
                    schema_to_field_with_props(
                        &field.schema,
                        Some(&format!("{}.{}", name.fullname(None), field.name)),
                        false,
                        Some(&props),
                    )
                })
                .collect();
            DataType::Struct(fields?)
        }
        AvroSchema::Enum { symbols, name, .. } => {
            return Ok(Field::new_dict(
                &name.fullname(None),
                index_type(symbols.len()),
                false,
                0,
                false,
            ))
        }
        AvroSchema::Fixed { size, .. } => DataType::FixedSizeBinary(*size as i32),
        AvroSchema::Decimal {
            precision, scale, ..
        } => DataType::Decimal128(*precision, *scale),
        AvroSchema::Uuid => DataType::FixedSizeBinary(16),
        AvroSchema::Date => DataType::Date32,
        AvroSchema::TimeMillis => DataType::Time32(TimeUnit::Millisecond),
        AvroSchema::TimeMicros => DataType::Time64(TimeUnit::Microsecond),
        AvroSchema::TimestampMillis => DataType::Timestamp(TimeUnit::Millisecond, None),
        AvroSchema::TimestampMicros => DataType::Timestamp(TimeUnit::Microsecond, None),
        AvroSchema::Duration => DataType::Duration(TimeUnit::Millisecond),
    };

    let data_type = field_type.clone();
    let name = name.unwrap_or_else(|| default_field_name(&data_type));

    let mut field = Field::new(name, field_type, nullable);
    field.set_metadata(props.cloned());
    Ok(field)
}

fn default_field_name(dt: &DataType) -> &str {
    match dt {
        DataType::Null => "null",
        DataType::Boolean => "bit",
        DataType::Int8 => "tinyint",
        DataType::Int16 => "smallint",
        DataType::Int32 => "int",
        DataType::Int64 => "bigint",
        DataType::UInt8 => "uint1",
        DataType::UInt16 => "uint2",
        DataType::UInt32 => "uint4",
        DataType::UInt64 => "uint8",
        DataType::Float16 => "float2",
        DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Date32 => "dateday",
        DataType::Date64 => "datemilli",
        DataType::Time32(tu) | DataType::Time64(tu) => match tu {
            TimeUnit::Second => "timesec",
            TimeUnit::Millisecond => "timemilli",
            TimeUnit::Microsecond => "timemicro",
            TimeUnit::Nanosecond => "timenano",
        },
        DataType::Timestamp(tu, tz) => {
            if tz.is_some() {
                match tu {
                    TimeUnit::Second => "timestampsectz",
                    TimeUnit::Millisecond => "timestampmillitz",
                    TimeUnit::Microsecond => "timestampmicrotz",
                    TimeUnit::Nanosecond => "timestampnanotz",
                }
            } else {
                match tu {
                    TimeUnit::Second => "timestampsec",
                    TimeUnit::Millisecond => "timestampmilli",
                    TimeUnit::Microsecond => "timestampmicro",
                    TimeUnit::Nanosecond => "timestampnano",
                }
            }
        }
        DataType::Duration(_) => "duration",
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => "intervalyear",
            IntervalUnit::DayTime => "intervalmonth",
            IntervalUnit::MonthDayNano => "intervalmonthdaynano",
        },
        DataType::Binary => "varbinary",
        DataType::FixedSizeBinary(_) => "fixedsizebinary",
        DataType::LargeBinary => "largevarbinary",
        DataType::Utf8 => "varchar",
        DataType::LargeUtf8 => "largevarchar",
        DataType::List(_) => "list",
        DataType::FixedSizeList(_, _) => "fixed_size_list",
        DataType::LargeList(_) => "largelist",
        DataType::Struct(_) => "struct",
        DataType::Union(_, _, _) => "union",
        DataType::Dictionary(_, _) => "map",
        DataType::Map(_, _) => unimplemented!("Map support not implemented"),
        DataType::Decimal128(_, _) => "decimal",
        DataType::Decimal256(_, _) => "decimal",
    }
}

fn index_type(len: usize) -> DataType {
    if len <= usize::from(u8::MAX) {
        DataType::Int8
    } else if len <= usize::from(u16::MAX) {
        DataType::Int16
    } else if usize::try_from(u32::MAX).map(|i| len < i).unwrap_or(false) {
        DataType::Int32
    } else {
        DataType::Int64
    }
}

fn external_props(schema: &AvroSchema) -> BTreeMap<String, String> {
    let mut props = BTreeMap::new();
    match &schema {
        AvroSchema::Record {
            doc: Some(ref doc), ..
        }
        | AvroSchema::Enum {
            doc: Some(ref doc), ..
        } => {
            props.insert("avro::doc".to_string(), doc.clone());
        }
        _ => {}
    }
    match &schema {
        AvroSchema::Record {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        }
        | AvroSchema::Enum {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        }
        | AvroSchema::Fixed {
            name:
                Name {
                    aliases: Some(aliases),
                    namespace,
                    ..
                },
            ..
        } => {
            let aliases: Vec<String> = aliases
                .iter()
                .map(|alias| aliased(alias, namespace.as_deref(), None))
                .collect();
            props.insert(
                "avro::aliases".to_string(),
                format!("[{}]", aliases.join(",")),
            );
        }
        _ => {}
    }
    props
}

#[allow(dead_code)]
fn get_metadata(
    _schema: AvroSchema,
    props: BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut metadata: BTreeMap<String, String> = Default::default();
    metadata.extend(props);
    metadata
}

/// Returns the fully qualified name for a field
pub fn aliased(
    name: &str,
    namespace: Option<&str>,
    default_namespace: Option<&str>,
) -> String {
    if name.contains('.') {
        name.to_string()
    } else {
        let namespace = namespace.as_ref().copied().or(default_namespace);

        match namespace {
            Some(ref namespace) => format!("{}.{}", namespace, name),
            None => name.to_string(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::{aliased, external_props, to_arrow_schema};
    use crate::arrow::datatypes::DataType::{Binary, Float32, Float64, Timestamp, Utf8};
    use crate::arrow::datatypes::TimeUnit::Microsecond;
    use crate::arrow::datatypes::{Field, Schema};
    use arrow::datatypes::DataType::{Boolean, Int32, Int64};
    use avro_rs::schema::Name;
    use avro_rs::Schema as AvroSchema;

    #[test]
    fn test_alias() {
        assert_eq!(aliased("foo.bar", None, None), "foo.bar");
        assert_eq!(aliased("bar", Some("foo"), None), "foo.bar");
        assert_eq!(aliased("bar", Some("foo"), Some("cat")), "foo.bar");
        assert_eq!(aliased("bar", None, Some("cat")), "cat.bar");
    }

    #[test]
    fn test_external_props() {
        let record_schema = AvroSchema::Record {
            name: Name {
                name: "record".to_string(),
                namespace: None,
                aliases: Some(vec!["fooalias".to_string(), "baralias".to_string()]),
            },
            doc: Some("record documentation".to_string()),
            fields: vec![],
            lookup: Default::default(),
        };
        let props = external_props(&record_schema);
        assert_eq!(
            props.get("avro::doc"),
            Some(&"record documentation".to_string())
        );
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[fooalias,baralias]".to_string())
        );
        let enum_schema = AvroSchema::Enum {
            name: Name {
                name: "enum".to_string(),
                namespace: None,
                aliases: Some(vec!["fooenum".to_string(), "barenum".to_string()]),
            },
            doc: Some("enum documentation".to_string()),
            symbols: vec![],
        };
        let props = external_props(&enum_schema);
        assert_eq!(
            props.get("avro::doc"),
            Some(&"enum documentation".to_string())
        );
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[fooenum,barenum]".to_string())
        );
        let fixed_schema = AvroSchema::Fixed {
            name: Name {
                name: "fixed".to_string(),
                namespace: None,
                aliases: Some(vec!["foofixed".to_string(), "barfixed".to_string()]),
            },
            size: 1,
        };
        let props = external_props(&fixed_schema);
        assert_eq!(
            props.get("avro::aliases"),
            Some(&"[foofixed,barfixed]".to_string())
        );
    }

    #[test]
    fn test_invalid_avro_schema() {}

    #[test]
    fn test_plain_types_schema() {
        let schema = AvroSchema::parse_str(
            r#"
            {
              "type" : "record",
              "name" : "topLevelRecord",
              "fields" : [ {
                "name" : "id",
                "type" : [ "int", "null" ]
              }, {
                "name" : "bool_col",
                "type" : [ "boolean", "null" ]
              }, {
                "name" : "tinyint_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "smallint_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "int_col",
                "type" : [ "int", "null" ]
              }, {
                "name" : "bigint_col",
                "type" : [ "long", "null" ]
              }, {
                "name" : "float_col",
                "type" : [ "float", "null" ]
              }, {
                "name" : "double_col",
                "type" : [ "double", "null" ]
              }, {
                "name" : "date_string_col",
                "type" : [ "bytes", "null" ]
              }, {
                "name" : "string_col",
                "type" : [ "bytes", "null" ]
              }, {
                "name" : "timestamp_col",
                "type" : [ {
                  "type" : "long",
                  "logicalType" : "timestamp-micros"
                }, "null" ]
              } ]
            }"#,
        );
        assert!(schema.is_ok(), "{:?}", schema);
        let arrow_schema = to_arrow_schema(&schema.unwrap());
        assert!(arrow_schema.is_ok(), "{:?}", arrow_schema);
        let expected = Schema::new(vec![
            Field::new("id", Int32, true),
            Field::new("bool_col", Boolean, true),
            Field::new("tinyint_col", Int32, true),
            Field::new("smallint_col", Int32, true),
            Field::new("int_col", Int32, true),
            Field::new("bigint_col", Int64, true),
            Field::new("float_col", Float32, true),
            Field::new("double_col", Float64, true),
            Field::new("date_string_col", Binary, true),
            Field::new("string_col", Binary, true),
            Field::new("timestamp_col", Timestamp(Microsecond, None), true),
        ]);
        assert_eq!(arrow_schema.unwrap(), expected);
    }

    #[test]
    fn test_non_record_schema() {
        let arrow_schema = to_arrow_schema(&AvroSchema::String);
        assert!(arrow_schema.is_ok(), "{:?}", arrow_schema);
        assert_eq!(
            arrow_schema.unwrap(),
            Schema::new(vec![Field::new("", Utf8, false)])
        );
    }
}
