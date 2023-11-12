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

//! Utility functions for complex field access

use arrow::datatypes::{DataType, Field};
use datafusion_common::{
    logical_type::LogicalType, plan_datafusion_err, plan_err, DFField, DataFusionError,
    Result, ScalarValue,
};

/// Types of the field access expression of a nested type, such as `Field` or `List`
pub enum GetFieldAccessSchema {
    /// Named field, For example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// Single list index, for example: `list[i]`
    ListIndex,
    /// List range, for example `list[i:j]`
    ListRange,
}

impl GetFieldAccessSchema {
    /// Returns the schema [`Field`] from a [`DataType::List`] or
    /// [`DataType::Struct`] indexed by this structure
    ///
    /// # Error
    /// Errors if
    /// * the `data_type` is not a Struct or a List,
    /// * the `data_type` of the name/index/start-stop do not match a supported index type
    pub fn get_accessed_field(&self, data_type: &DataType) -> Result<Field> {
        match self {
            Self::NamedStructField{ name } => {
                match (data_type, name) {
                    (DataType::Map(fields, _), _) => {
                        match fields.data_type() {
                            DataType::Struct(fields) if fields.len() == 2 => {
                                // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                                // often named "key", and "value", but we don't require any specific naming here;
                                // instead, we assume that the second columnis the "value" column both here and in
                                // execution.
                                let value_field = fields.get(1).expect("fields should have exactly two members");
                                Ok(Field::new("map", value_field.data_type().clone(), true))
                            },
                            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
                        }
                    }
                    (DataType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
                        if s.is_empty() {
                            plan_err!(
                                "Struct based indexed access requires a non empty string"
                            )
                        } else {
                            let field = fields.iter().find(|f| f.name() == s);
                            field.ok_or(plan_datafusion_err!("Field {s} not found in struct")).map(|f| f.as_ref().clone())
                        }
                    }
                    (DataType::Struct(_), _) => plan_err!(
                        "Only utf8 strings are valid as an indexed field in a struct"
                    ),
                    (other, _) => plan_err!("The expression to get an indexed field is only valid for `List`, `Struct`, or `Map` types, got {other}"),
                }
            }
            Self::ListIndex => {
                match data_type {
                    DataType::List(lt) => Ok(Field::new("list", lt.data_type().clone(), true)),
                    other => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
                }
            }
            Self::ListRange => {
                match data_type {
                    DataType::List(_) => Ok(Field::new("list", data_type.clone(), true)),
                    other => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
                }
            }
        }
    }
    /// Returns the schema [`DFField`] from a [`LogicalType::List`] or
    /// [`LogicalType::Struct`] indexed by this structure
    ///
    /// # Error
    /// Errors if
    /// * the `data_type` is not a Struct or a List,
    /// * the `data_type` of the name/index/start-stop do not match a supported index type
    pub fn get_accessed_df_field(&self, data_type: &LogicalType) -> Result<DFField> {
        match self {
            Self::NamedStructField{ name } => {
                match (data_type, name) {
                    (LogicalType::Map(fields, _), _) => {
                        match fields.data_type() {
                            LogicalType::Struct(fields) if fields.len() == 2 => {
                                // Arrow's MapArray is essentially a ListArray of structs with two columns. They are
                                // often named "key", and "value", but we don't require any specific naming here;
                                // instead, we assume that the second columnis the "value" column both here and in
                                // execution.
                                let value_field = fields.get(1).expect("fields should have exactly two members");
                                Ok(DFField::new_unqualified("map", value_field.data_type().clone(), true))
                            },
                            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
                        }
                    }
                    (LogicalType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
                        if s.is_empty() {
                            plan_err!(
                                "Struct based indexed access requires a non empty string"
                            )
                        } else {
                            let field = fields.iter().find(|f| f.name() == s);
                            field.ok_or(plan_datafusion_err!("Field {s} not found in struct")).map(|f| {
                                DFField::new_unqualified(f.name(), f.data_type().clone(), true)
                            })
                        }
                    }
                    (LogicalType::Struct(_), _) => plan_err!(
                        "Only utf8 strings are valid as an indexed field in a struct"
                    ),
                    (other, _) => plan_err!("The expression to get an indexed field is only valid for `List`, `Struct`, or `Map` types, got {other}"),
                }
            }
            Self::ListIndex => {
                match data_type {
                    LogicalType::List(lt) => Ok(DFField::new_unqualified("list", lt.as_ref().clone(), true)),
                    other => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
                }
            }
            Self::ListRange => {
                match data_type {
                    LogicalType::List(_) => Ok(DFField::new_unqualified("list", data_type.clone(), true)),
                    other => plan_err!("The expression to get an indexed field is only valid for `List` or `Struct` types, got {other}"),
                }
            }
        }
    }
}
