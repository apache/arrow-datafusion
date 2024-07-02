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

//! [`ScalarStructBuilder`] for building [`ScalarValue::Struct`]

use crate::error::_internal_err;
use crate::{Result, ScalarValue};
use arrow::array::{ArrayRef, StructArray};
use arrow::datatypes::{DataType, FieldRef, Fields};
use arrow_schema::Field;
use std::sync::Arc;

/// Builder for [`ScalarValue::Struct`].
///
/// See examples on [`ScalarValue`]
#[derive(Debug, Default)]
pub struct ScalarStructBuilder {
    fields: Vec<FieldRef>,
    arrays: Vec<ArrayRef>,
}

impl ScalarStructBuilder {
    /// Create a new `ScalarStructBuilder`
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a new [`ScalarValue::Struct`] with a single `null` value.
    ///
    /// Note this is different from a struct where each of the specified fields
    /// are null (e.g. `{a: NULL}`)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::scalar::ScalarStructBuilder;
    /// let fields = vec![
    ///    Field::new("a", DataType::Int32, false),
    /// ];
    /// let sv = ScalarStructBuilder::new_null(fields);
    /// // Note this is `NULL`, not `{a: NULL}`
    /// assert_eq!(format!("{sv}"), "NULL");
    ///```
    ///
    /// To create a struct where the *fields* are null, use `Self::new()` and
    /// pass null values for each field:
    ///
    /// ```rust
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::scalar::{ScalarStructBuilder, ScalarValue};
    /// // make a nullable field
    /// let field = Field::new("a", DataType::Int32, true);
    /// // add a null value for the "a" field
    /// let sv = ScalarStructBuilder::new()
    ///   .with_scalar(field, ScalarValue::Int32(None))
    ///   .build()
    ///   .unwrap();
    /// // value is not null, but field is
    /// assert_eq!(format!("{sv}"), "{a:}");
    /// ```
    pub fn new_null(fields: impl IntoFields) -> ScalarValue {
        DataType::Struct(fields.into()).try_into().unwrap()
    }

    /// Add the specified field and [`ArrayRef`] to the struct.
    ///
    /// Note the array should have a single row.
    pub fn with_array(mut self, field: impl IntoFieldRef, value: ArrayRef) -> Self {
        self.fields.push(field.into_field_ref());
        self.arrays.push(value);
        self
    }

    /// Add the specified field and `ScalarValue` to the struct.
    pub fn with_scalar(self, field: impl IntoFieldRef, value: ScalarValue) -> Self {
        // valid scalar value should not fail
        let array = value.to_array().unwrap();
        self.with_array(field, array)
    }

    /// Add a field with the specified name and value to the struct.
    /// the field is created with the specified data type and as non nullable
    pub fn with_name_and_scalar(self, name: &str, value: ScalarValue) -> Self {
        let field = Field::new(name, value.data_type(), false);
        self.with_scalar(field, value)
    }

    /// Return a [`ScalarValue::Struct`] with the fields and values added so far
    ///
    /// # Errors
    ///
    /// If the [`StructArray`] cannot be created (for example if there is a
    /// mismatch between field types and arrays) or the arrays do not have
    /// exactly one element.
    pub fn build(self) -> Result<ScalarValue> {
        let Self { fields, arrays } = self;

        for array in &arrays {
            if array.len() != 1 {
                return _internal_err!(
                    "Error building ScalarValue::Struct. \
                Expected array with exactly one element, found array with {} elements",
                    array.len()
                );
            }
        }

        let struct_array = StructArray::try_new(Fields::from(fields), arrays, None)?;
        Ok(ScalarValue::Struct(Arc::new(struct_array)))
    }
}

/// Trait for converting a type into a [`FieldRef`]
///
/// Used to avoid having to call `clone()` on a `FieldRef` when adding a field to
/// a `ScalarStructBuilder`.
///
/// TODO potentially upstream this to arrow-rs so that we can
/// use impl `Into<FieldRef>` instead
pub trait IntoFieldRef {
    fn into_field_ref(self) -> FieldRef;
}

impl IntoFieldRef for FieldRef {
    fn into_field_ref(self) -> FieldRef {
        self
    }
}

impl IntoFieldRef for &FieldRef {
    fn into_field_ref(self) -> FieldRef {
        Arc::clone(self)
    }
}

impl IntoFieldRef for Field {
    fn into_field_ref(self) -> FieldRef {
        FieldRef::new(self)
    }
}

/// Trait for converting a type into a [`Fields`]
///
/// This avoids to avoid having to call clone() on an Arc'd `Fields` when adding
/// a field to a `ScalarStructBuilder`
///
/// TODO potentially upstream this to arrow-rs so that we can
/// use impl `Into<Fields>` instead
pub trait IntoFields {
    fn into(self) -> Fields;
}

impl IntoFields for Fields {
    fn into(self) -> Fields {
        self
    }
}

impl IntoFields for &Fields {
    fn into(self) -> Fields {
        self.clone()
    }
}

impl IntoFields for Vec<Field> {
    fn into(self) -> Fields {
        Fields::from(self)
    }
}
