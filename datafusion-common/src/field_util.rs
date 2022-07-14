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

use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, Metadata, Schema};
use arrow::error::Error as ArrowError;
use std::borrow::Borrow;
use std::collections::BTreeMap;

use crate::ScalarValue;
use crate::{DataFusionError, Result};

/// Returns the field access indexed by `key` from a [`DataType::List`] or [`DataType::Struct`]
/// # Error
/// Errors if
/// * the `data_type` is not a Struct or,
/// * there is no field key is not of the required index type
pub fn get_indexed_field(data_type: &DataType, key: &ScalarValue) -> Result<Field> {
    match (data_type, key) {
        (DataType::List(lt), ScalarValue::Int64(Some(i))) => {
            if *i < 0 {
                Err(DataFusionError::Plan(format!(
                    "List based indexed access requires a positive int, was {0}",
                    i
                )))
            } else {
                Ok(Field::new(&i.to_string(), lt.data_type().clone(), false))
            }
        }
        (DataType::Struct(fields), ScalarValue::Utf8(Some(s))) => {
            if s.is_empty() {
                Err(DataFusionError::Plan(
                    "Struct based indexed access requires a non empty string".to_string(),
                ))
            } else {
                let field = fields.iter().find(|f| f.name() == s);
                match field {
                    None => Err(DataFusionError::Plan(format!(
                        "Field {} not found in struct",
                        s
                    ))),
                    Some(f) => Ok(f.clone()),
                }
            }
        }
        (DataType::Struct(_), _) => Err(DataFusionError::Plan(
            "Only utf8 strings are valid as an indexed field in a struct".to_string(),
        )),
        (DataType::List(_), _) => Err(DataFusionError::Plan(
            "Only ints are valid as an indexed field in a list".to_string(),
        )),
        _ => Err(DataFusionError::Plan(
            "The expression to get an indexed field is only valid for `List` types"
                .to_string(),
        )),
    }
}

/// Imitate arrow-rs StructArray behavior by extending arrow2 StructArray
pub trait StructArrayExt {
    /// Return field names in this struct array
    fn column_names(&self) -> Vec<&str>;
    /// Return child array whose field name equals to column_name
    fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef>;
    /// Return the number of fields in this struct array
    fn num_columns(&self) -> usize;
    /// Return the column at the position
    fn column(&self, pos: usize) -> ArrayRef;
}

impl StructArrayExt for StructArray {
    fn column_names(&self) -> Vec<&str> {
        self.fields().iter().map(|f| f.name.as_str()).collect()
    }

    fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef> {
        self.fields()
            .iter()
            .position(|c| c.name() == column_name)
            .map(|pos| self.values()[pos].borrow())
    }

    fn num_columns(&self) -> usize {
        self.fields().len()
    }

    fn column(&self, pos: usize) -> ArrayRef {
        self.values()[pos].clone()
    }
}

/// Converts a list of field / array pairs to a struct array
pub fn struct_array_from(pairs: Vec<(Field, ArrayRef)>) -> StructArray {
    let fields: Vec<Field> = pairs.iter().map(|v| v.0.clone()).collect();
    let values = pairs.iter().map(|v| v.1.clone()).collect();
    StructArray::from_data(DataType::Struct(fields), values, None)
}

/// Imitate arrow-rs Schema behavior by extending arrow2 Schema
pub trait SchemaExt {
    /// Creates a new [`Schema`] from a sequence of [`Field`] values.
    ///
    /// # Example
    ///
    /// ```
    /// use arrow::datatypes::{Field, DataType, Schema};
    /// use datafusion_common::field_util::SchemaExt;
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let schema = Schema::new(vec![field_a, field_b]);
    /// ```
    fn new(fields: Vec<Field>) -> Self;

    /// Creates a new [`Schema`] from a sequence of [`Field`] values and [`arrow::datatypes::Metadata`]
    ///
    /// # Example
    ///
    /// ```
    /// use std::collections::BTreeMap;
    /// use arrow::datatypes::{Field, DataType, Schema};
    /// use datafusion_common::field_util::SchemaExt;
    ///
    /// let field_a = Field::new("a", DataType::Int64, false);
    /// let field_b = Field::new("b", DataType::Boolean, false);
    ///
    /// let schema_metadata: BTreeMap<String, String> =
    ///             vec![("baz".to_string(), "barf".to_string())]
    ///                 .into_iter()
    ///                 .collect();
    /// let schema = Schema::new_with_metadata(vec![field_a, field_b], schema_metadata);
    /// ```
    fn new_with_metadata(fields: Vec<Field>, metadata: Metadata) -> Self;

    /// Creates an empty [`Schema`].
    fn empty() -> Self;

    /// Look up a column by name and return a immutable reference to the column along with
    /// its index.
    fn column_with_name(&self, name: &str) -> Option<(usize, &Field)>;

    /// Returns the first [`Field`] named `name`.
    fn field_with_name(&self, name: &str) -> Result<&Field>;

    /// Find the index of the column with the given name.
    fn index_of(&self, name: &str) -> Result<usize>;

    /// Returns the [`Field`] at position `i`.
    /// # Panics
    /// Panics iff `i` is larger than the number of fields in this [`Schema`].
    fn field(&self, index: usize) -> &Field;

    /// Returns all [`Field`]s in this schema.
    fn fields(&self) -> &[Field];

    /// Returns an immutable reference to the Map of custom metadata key-value pairs.
    fn metadata(&self) -> &BTreeMap<String, String>;

    /// Merge schema into self if it is compatible. Struct fields will be merged recursively.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow::datatypes::*;
    /// use datafusion_common::field_util::SchemaExt;
    ///
    /// let merged = Schema::try_merge(vec![
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, false),
    ///         Field::new("c2", DataType::Utf8, false),
    ///     ]),
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// ]).unwrap();
    ///
    /// assert_eq!(
    ///     merged,
    ///     Schema::new(vec![
    ///         Field::new("c1", DataType::Int64, true),
    ///         Field::new("c2", DataType::Utf8, false),
    ///         Field::new("c3", DataType::Utf8, false),
    ///     ]),
    /// );
    /// ```
    fn try_merge(schemas: impl IntoIterator<Item = Self>) -> Result<Self>
    where
        Self: Sized;

    /// Return the field names
    fn field_names(&self) -> Vec<String>;

    /// Returns a new schema with only the specified columns in the new schema
    /// This carries metadata from the parent schema over as well
    fn project(&self, indices: &[usize]) -> Result<Schema>;
}

impl SchemaExt for Schema {
    fn new(fields: Vec<Field>) -> Self {
        Self::from(fields)
    }

    fn new_with_metadata(fields: Vec<Field>, metadata: Metadata) -> Self {
        Self::new(fields).with_metadata(metadata)
    }

    fn empty() -> Self {
        Self::from(vec![])
    }

    fn column_with_name(&self, name: &str) -> Option<(usize, &Field)> {
        self.fields.iter().enumerate().find(|(_, f)| f.name == name)
    }

    fn field_with_name(&self, name: &str) -> Result<&Field> {
        Ok(&self.fields[self.index_of(name)?])
    }

    fn index_of(&self, name: &str) -> Result<usize> {
        self.column_with_name(name).map(|(i, _f)| i).ok_or_else(|| {
            DataFusionError::ArrowError(ArrowError::InvalidArgumentError(format!(
                "Unable to get field named \"{}\". Valid fields: {:?}",
                name,
                self.field_names()
            )))
        })
    }

    fn field(&self, index: usize) -> &Field {
        &self.fields[index]
    }

    #[inline]
    fn fields(&self) -> &[Field] {
        &self.fields
    }

    #[inline]
    fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    fn try_merge(schemas: impl IntoIterator<Item = Self>) -> Result<Self> {
        schemas
            .into_iter()
            .try_fold(Self::empty(), |mut merged, schema| {
                let Schema { metadata, fields } = schema;
                for (key, value) in metadata.into_iter() {
                    // merge metadata
                    if let Some(old_val) = merged.metadata.get(&key) {
                        if old_val != &value {
                            return Err(DataFusionError::ArrowError(
                                ArrowError::InvalidArgumentError(
                                    "Fail to merge schema due to conflicting metadata."
                                        .to_string(),
                                ),
                            ));
                        }
                    }
                    merged.metadata.insert(key, value);
                }
                // merge fields
                for field in fields.into_iter() {
                    let mut new_field = true;
                    for merged_field in &mut merged.fields {
                        if field.name() != merged_field.name() {
                            continue;
                        }
                        new_field = false;
                        merged_field.try_merge(&field)?
                    }
                    // found a new field, add to field list
                    if new_field {
                        merged.fields.push(field);
                    }
                }
                Ok(merged)
            })
    }

    fn field_names(&self) -> Vec<String> {
        self.fields.iter().map(|f| f.name.to_string()).collect()
    }

    fn project(&self, indices: &[usize]) -> Result<Schema> {
        let new_fields = indices
            .iter()
            .map(|i| {
                self.fields.get(*i).cloned().ok_or_else(|| {
                    DataFusionError::ArrowError(ArrowError::InvalidArgumentError(
                        format!(
                            "project index {} out of bounds, max field {}",
                            i,
                            self.fields().len()
                        ),
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new_with_metadata(new_fields, self.metadata.clone()))
    }
}

/// Imitate arrow-rs Field behavior by extending arrow2 Field
pub trait FieldExt {
    /// The field name
    fn name(&self) -> &str;

    /// Whether the field is nullable
    fn is_nullable(&self) -> bool;

    /// Returns the field metadata
    fn metadata(&self) -> &BTreeMap<String, String>;

    /// Merge field into self if it is compatible. Struct will be merged recursively.
    /// NOTE: `self` may be updated to unexpected state in case of merge failure.
    ///
    /// Example:
    ///
    /// ```
    /// use arrow2::datatypes::*;
    ///
    /// let mut field = Field::new("c1", DataType::Int64, false);
    /// assert!(field.try_merge(&Field::new("c1", DataType::Int64, true)).is_ok());
    /// assert!(field.is_nullable());
    /// ```
    fn try_merge(&mut self, from: &Field) -> Result<()>;

    /// Sets the `Field`'s optional custom metadata.
    /// The metadata is set as `None` for empty map.
    fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>);
}

impl FieldExt for Field {
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    #[inline]
    fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    fn try_merge(&mut self, from: &Field) -> Result<()> {
        // merge metadata
        for (key, from_value) in from.metadata() {
            if let Some(self_value) = self.metadata.get(key) {
                if self_value != from_value {
                    return Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(format!(
                        "Fail to merge field due to conflicting metadata data value for key {}",
                        key
                    ))));
                }
            } else {
                self.metadata.insert(key.clone(), from_value.clone());
            }
        }

        match &mut self.data_type {
            DataType::Struct(nested_fields) => match &from.data_type {
                DataType::Struct(from_nested_fields) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if self_field.name != from_field.name {
                                continue;
                            }
                            is_new_field = false;
                            self_field.try_merge(from_field)?;
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(DataFusionError::ArrowError(
                        ArrowError::InvalidArgumentError(
                            "Fail to merge schema Field due to conflicting datatype"
                                .to_string(),
                        ),
                    ));
                }
            },
            DataType::Union(nested_fields, _, _) => match &from.data_type {
                DataType::Union(from_nested_fields, _, _) => {
                    for from_field in from_nested_fields {
                        let mut is_new_field = true;
                        for self_field in nested_fields.iter_mut() {
                            if from_field == self_field {
                                is_new_field = false;
                                break;
                            }
                        }
                        if is_new_field {
                            nested_fields.push(from_field.clone());
                        }
                    }
                }
                _ => {
                    return Err(DataFusionError::ArrowError(
                        ArrowError::InvalidArgumentError(
                            "Fail to merge schema Field due to conflicting datatype"
                                .to_string(),
                        ),
                    ));
                }
            },
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::LargeList(_)
            | DataType::List(_)
            | DataType::Dictionary(_, _, _)
            | DataType::FixedSizeList(_, _)
            | DataType::FixedSizeBinary(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Extension(_, _, _)
            | DataType::Map(_, _)
            | DataType::Decimal(_, _) => {
                if self.data_type != from.data_type {
                    return Err(DataFusionError::ArrowError(
                        ArrowError::InvalidArgumentError(
                            "Fail to merge schema Field due to conflicting datatype"
                                .to_string(),
                        ),
                    ));
                }
            }
        }
        if from.is_nullable {
            self.is_nullable = from.is_nullable;
        }

        Ok(())
    }

    #[inline]
    fn set_metadata(&mut self, metadata: Option<BTreeMap<String, String>>) {
        if let Some(v) = metadata {
            if !v.is_empty() {
                self.metadata = v;
            }
        }
    }
}
