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

//! DFSchema is an extended schema struct that DataFusion uses to provide support for
//! fields with optional relation names.

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::error::{
    unqualified_field_not_found, DataFusionError, Result, SchemaError, _plan_err,
};
use crate::logical_type::{ExtensionType, LogicalType};
use crate::{
    field_not_found, Column, FunctionalDependencies, OwnedTableReference, TableReference,
};

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};

/// A reference-counted reference to a `DFSchema`.
pub type DFSchemaRef = Arc<DFSchema>;

/// DFSchema wraps an Arrow schema and adds relation names
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFSchema {
    /// Fields
    fields: Vec<DFField>,
    /// Additional metadata in form of key value pairs
    metadata: HashMap<String, String>,
    /// Stores functional dependencies in the schema.
    functional_dependencies: FunctionalDependencies,
}

impl DFSchema {
    /// Creates an empty `DFSchema`
    pub fn empty() -> Self {
        Self {
            fields: vec![],
            metadata: HashMap::new(),
            functional_dependencies: FunctionalDependencies::empty(),
        }
    }

    #[deprecated(since = "7.0.0", note = "please use `new_with_metadata` instead")]
    /// Create a new `DFSchema`
    pub fn new(fields: Vec<DFField>) -> Result<Self> {
        Self::new_with_metadata(fields, HashMap::new())
    }

    /// Create a new `DFSchema`
    pub fn new_with_metadata(
        fields: Vec<DFField>,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        let mut qualified_names = HashSet::new();
        let mut unqualified_names = HashSet::new();

        for field in &fields {
            if let Some(qualifier) = field.qualifier() {
                qualified_names.insert((qualifier, field.name()));
            } else if !unqualified_names.insert(field.name()) {
                return Err(DataFusionError::SchemaError(
                    SchemaError::DuplicateUnqualifiedField {
                        name: field.name().to_string(),
                    },
                ));
            }
        }

        // check for mix of qualified and unqualified field with same unqualified name
        // note that we need to sort the contents of the HashSet first so that errors are
        // deterministic
        let mut qualified_names = qualified_names
            .iter()
            .map(|(l, r)| (l.to_owned(), r.to_owned()))
            .collect::<Vec<(&OwnedTableReference, &String)>>();
        qualified_names.sort();
        for (qualifier, name) in &qualified_names {
            if unqualified_names.contains(name) {
                return Err(DataFusionError::SchemaError(
                    SchemaError::AmbiguousReference {
                        field: Column {
                            relation: Some((*qualifier).clone()),
                            name: name.to_string(),
                        },
                    },
                ));
            }
        }
        Ok(Self {
            fields,
            metadata,
            functional_dependencies: FunctionalDependencies::empty(),
        })
    }

    /// Create a `DFSchema` from an Arrow schema and a given qualifier
    pub fn try_from_qualified_schema<'a>(
        qualifier: impl Into<TableReference<'a>>,
        schema: &Schema,
    ) -> Result<Self> {
        let qualifier: TableReference<'a> = qualifier.into();
        Self::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| DFField::from_qualified(qualifier.clone(), f.clone()))
                .collect(),
            schema.metadata().clone(),
        )
    }

    /// Create a `DFSchema` from an Arrow schema and a given qualifier
    pub fn try_from_qualified_dfschema<'a>(
        qualifier: impl Into<TableReference<'a>>,
        schema: &DFSchema,
    ) -> Result<Self> {
        let qualifier: TableReference<'a> = qualifier.into();
        Self::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| f.clone().with_qualifier(qualifier.to_owned_reference()))
                .collect(),
            schema.metadata().clone(),
        )
    }

    /// Assigns functional dependencies.
    pub fn with_functional_dependencies(
        mut self,
        functional_dependencies: FunctionalDependencies,
    ) -> Self {
        self.functional_dependencies = functional_dependencies;
        self
    }

    /// Create a new schema that contains the fields from this schema followed by the fields
    /// from the supplied schema. An error will be returned if there are duplicate field names.
    pub fn join(&self, schema: &DFSchema) -> Result<Self> {
        let mut fields = self.fields.clone();
        let mut metadata = self.metadata.clone();
        fields.extend_from_slice(schema.fields().as_slice());
        metadata.extend(schema.metadata.clone());
        Self::new_with_metadata(fields, metadata)
    }

    /// Modify this schema by appending the fields from the supplied schema, ignoring any
    /// duplicate fields.
    pub fn merge(&mut self, other_schema: &DFSchema) {
        if other_schema.fields.is_empty() {
            return;
        }
        for field in other_schema.fields() {
            // skip duplicate columns
            let duplicated_field = match field.qualifier() {
                Some(q) => self.field_with_name(Some(q), field.name()).is_ok(),
                // for unqualified columns, check as unqualified name
                None => self.field_with_unqualified_name(field.name()).is_ok(),
            };
            if !duplicated_field {
                self.fields.push(field.clone());
            }
        }
        self.metadata.extend(other_schema.metadata.clone())
    }

    /// Get a list of fields
    pub fn fields(&self) -> &Vec<DFField> {
        &self.fields
    }

    /// Returns an immutable reference of a specific `Field` instance selected using an
    /// offset within the internal `fields` vector
    pub fn field(&self, i: usize) -> &DFField {
        &self.fields[i]
    }

    #[deprecated(since = "8.0.0", note = "please use `index_of_column_by_name` instead")]
    /// Find the index of the column with the given unqualified name
    pub fn index_of(&self, name: &str) -> Result<usize> {
        for i in 0..self.fields.len() {
            if self.fields[i].name() == name {
                return Ok(i);
            } else {
                // Now that `index_of` is deprecated an error is thrown if
                // a fully qualified field name is provided.
                match &self.fields[i].qualifier {
                    Some(qualifier) => {
                        if (qualifier.to_string() + "." + self.fields[i].name()) == name {
                            return _plan_err!(
                                "Fully qualified field name '{name}' was supplied to `index_of` \
                                which is deprecated. Please use `index_of_column_by_name` instead"
                            );
                        }
                    }
                    None => (),
                }
            }
        }

        Err(unqualified_field_not_found(name, self))
    }

    pub fn index_of_column_by_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<Option<usize>> {
        let mut matches = self
            .fields
            .iter()
            .enumerate()
            .filter(|(_, field)| match (qualifier, &field.qualifier) {
                // field to lookup is qualified.
                // current field is qualified and not shared between relations, compare both
                // qualifier and name.
                (Some(q), Some(field_q)) => {
                    q.resolved_eq(field_q) && field.name() == name
                }
                // field to lookup is qualified but current field is unqualified.
                (Some(qq), None) => {
                    // the original field may now be aliased with a name that matches the
                    // original qualified name
                    let column = Column::from_qualified_name(field.name());
                    match column {
                        Column {
                            relation: Some(r),
                            name: column_name,
                        } => &r == qq && column_name == name,
                        _ => false,
                    }
                }
                // field to lookup is unqualified, no need to compare qualifier
                (None, Some(_)) | (None, None) => field.name() == name,
            })
            .map(|(idx, _)| idx);
        Ok(matches.next())
    }

    /// Find the index of the column with the given qualifier and name
    pub fn index_of_column(&self, col: &Column) -> Result<usize> {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)?
            .ok_or_else(|| field_not_found(col.relation.clone(), &col.name, self))
    }

    /// Check if the column is in the current schema
    pub fn is_column_from_schema(&self, col: &Column) -> Result<bool> {
        self.index_of_column_by_name(col.relation.as_ref(), &col.name)
            .map(|idx| idx.is_some())
    }

    /// Find the field with the given name
    pub fn field_with_name(
        &self,
        qualifier: Option<&TableReference>,
        name: &str,
    ) -> Result<&DFField> {
        if let Some(qualifier) = qualifier {
            self.field_with_qualified_name(qualifier, name)
        } else {
            self.field_with_unqualified_name(name)
        }
    }

    /// Find all fields having the given qualifier
    pub fn fields_with_qualified(&self, qualifier: &TableReference) -> Vec<&DFField> {
        self.fields
            .iter()
            .filter(|field| field.qualifier().map(|q| q.eq(qualifier)).unwrap_or(false))
            .collect()
    }

    /// Find all fields match the given name
    pub fn fields_with_unqualified_name(&self, name: &str) -> Vec<&DFField> {
        self.fields
            .iter()
            .filter(|field| field.name() == name)
            .collect()
    }

    /// Find the field with the given name
    pub fn field_with_unqualified_name(&self, name: &str) -> Result<&DFField> {
        let matches = self.fields_with_unqualified_name(name);
        match matches.len() {
            0 => Err(unqualified_field_not_found(name, self)),
            1 => Ok(matches[0]),
            _ => {
                // When `matches` size > 1, it doesn't necessarily mean an `ambiguous name` problem.
                // Because name may generate from Alias/... . It means that it don't own qualifier.
                // For example:
                //             Join on id = b.id
                // Project a.id as id   TableScan b id
                // In this case, there isn't `ambiguous name` problem. When `matches` just contains
                // one field without qualifier, we should return it.
                let fields_without_qualifier = matches
                    .iter()
                    .filter(|f| f.qualifier.is_none())
                    .collect::<Vec<_>>();
                if fields_without_qualifier.len() == 1 {
                    Ok(fields_without_qualifier[0])
                } else {
                    Err(DataFusionError::SchemaError(
                        SchemaError::AmbiguousReference {
                            field: Column {
                                relation: None,
                                name: name.to_string(),
                            },
                        },
                    ))
                }
            }
        }
    }

    /// Find the field with the given qualified name
    pub fn field_with_qualified_name(
        &self,
        qualifier: &TableReference,
        name: &str,
    ) -> Result<&DFField> {
        let idx = self
            .index_of_column_by_name(Some(qualifier), name)?
            .ok_or_else(|| field_not_found(Some(qualifier.to_string()), name, self))?;

        Ok(self.field(idx))
    }

    /// Find the field with the given qualified column
    pub fn field_from_column(&self, column: &Column) -> Result<&DFField> {
        match &column.relation {
            Some(r) => self.field_with_qualified_name(r, &column.name),
            None => self.field_with_unqualified_name(&column.name),
        }
    }

    /// Find if the field exists with the given name
    pub fn has_column_with_unqualified_name(&self, name: &str) -> bool {
        self.fields().iter().any(|field| field.name() == name)
    }

    /// Find if the field exists with the given qualified name
    pub fn has_column_with_qualified_name(
        &self,
        qualifier: &TableReference,
        name: &str,
    ) -> bool {
        self.fields().iter().any(|field| {
            field.qualifier().map(|q| q.eq(qualifier)).unwrap_or(false)
                && field.name() == name
        })
    }

    /// Find if the field exists with the given qualified column
    pub fn has_column(&self, column: &Column) -> bool {
        match &column.relation {
            Some(r) => self.has_column_with_qualified_name(r, &column.name),
            None => self.has_column_with_unqualified_name(&column.name),
        }
    }

    /// Check to see if unqualified field names matches field names in Arrow schema
    pub fn matches_arrow_schema(&self, arrow_schema: &Schema) -> bool {
        self.fields
            .iter()
            .zip(arrow_schema.fields().iter())
            .all(|(dffield, arrowfield)| dffield.name() == arrowfield.name())
    }

    /// Check to see if fields in 2 Arrow schemas are compatible
    pub fn check_arrow_schema_type_compatible(
        &self,
        arrow_schema: &Schema,
    ) -> Result<()> {
        let self_arrow_schema: Schema = self.into();
        self_arrow_schema
            .fields()
            .iter()
            .zip(arrow_schema.fields().iter())
            .try_for_each(|(l_field, r_field)| {
                if !can_cast_types(r_field.data_type(), l_field.data_type()) {
                    _plan_err!("Column {} (type: {}) is not compatible with column {} (type: {})",
                                r_field.name(),
                                r_field.data_type(),
                                l_field.name(),
                                l_field.data_type())
                } else {
                    Ok(())
                }
            })
    }

    /// Returns true if the two schemas have the same qualified named
    /// fields with logically equivalent data types. Returns false otherwise.
    ///
    /// Use [DFSchema]::equivalent_names_and_types for stricter semantic type
    /// equivalence checking.
    pub fn logically_equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }
        let self_fields = self.fields().iter();
        let other_fields = other.fields().iter();
        self_fields.zip(other_fields).all(|(f1, f2)| {
            f1.qualifier() == f2.qualifier()
                && f1.name() == f2.name()
                && Self::datatype_is_logically_equal(f1.data_type(), f2.data_type())
        })
    }

    /// Returns true if the two schemas have the same qualified named
    /// fields with the same data types. Returns false otherwise.
    ///
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    ///
    /// Use [DFSchema]::logically_equivalent_names_and_types for a weaker
    /// logical type checking, which for example would consider a dictionary
    /// encoded UTF8 array to be equivalent to a plain UTF8 array.
    pub fn equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }
        let self_fields = self.fields().iter();
        let other_fields = other.fields().iter();
        self_fields.zip(other_fields).all(|(f1, f2)| {
            f1.qualifier() == f2.qualifier()
                && f1.name() == f2.name()
                && Self::datatype_is_logically_equal(f1.data_type(), f2.data_type())
        })
    }

    /// Checks if two [`DataType`]s are logically equal. This is a notably weaker constraint
    /// than datatype_is_semantically_equal in that a Dictionary<K,V> type is logically
    /// equal to a plain V type, but not semantically equal. Dictionary<K1, V1> is also
    /// logically equal to Dictionary<K2, V1>.
    fn datatype_is_logically_equal(dt1: &LogicalType, dt2: &LogicalType) -> bool {
        dt1 == dt2
    }

    /// Returns true of two [`DataType`]s are semantically equal (same
    /// name and type), ignoring both metadata and nullability.
    ///
    /// request to upstream: <https://github.com/apache/arrow-rs/issues/3199>
    fn datatype_is_semantically_equal(dt1: &DataType, dt2: &DataType) -> bool {
        // check nested fields
        match (dt1, dt2) {
            (DataType::Dictionary(k1, v1), DataType::Dictionary(k2, v2)) => {
                Self::datatype_is_semantically_equal(k1.as_ref(), k2.as_ref())
                    && Self::datatype_is_semantically_equal(v1.as_ref(), v2.as_ref())
            }
            (DataType::List(f1), DataType::List(f2))
            | (DataType::LargeList(f1), DataType::LargeList(f2))
            | (DataType::FixedSizeList(f1, _), DataType::FixedSizeList(f2, _))
            | (DataType::Map(f1, _), DataType::Map(f2, _)) => {
                Self::field_is_semantically_equal(f1, f2)
            }
            (DataType::Struct(fields1), DataType::Struct(fields2)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                        // all fields have to be the same
                    iter1
                    .zip(iter2)
                        .all(|(f1, f2)| Self::field_is_semantically_equal(f1, f2))
            }
            (DataType::Union(fields1, _), DataType::Union(fields2, _)) => {
                let iter1 = fields1.iter();
                let iter2 = fields2.iter();
                fields1.len() == fields2.len() &&
                    // all fields have to be the same
                    iter1
                        .zip(iter2)
                        .all(|((t1, f1), (t2, f2))| t1 == t2 && Self::field_is_semantically_equal(f1, f2))
            }
            (
                DataType::Decimal128(_l_precision, _l_scale),
                DataType::Decimal128(_r_precision, _r_scale),
            ) => true,
            (
                DataType::Decimal256(_l_precision, _l_scale),
                DataType::Decimal256(_r_precision, _r_scale),
            ) => true,
            _ => dt1 == dt2,
        }
    }

    fn field_is_semantically_equal(f1: &Field, f2: &Field) -> bool {
        f1.name() == f2.name()
            && Self::datatype_is_semantically_equal(f1.data_type(), f2.data_type())
    }

    /// Strip all field qualifier in schema
    pub fn strip_qualifiers(self) -> Self {
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| f.strip_qualifier())
                .collect(),
            ..self
        }
    }

    /// Replace all field qualifier with new value in schema
    pub fn replace_qualifier(self, qualifier: impl Into<OwnedTableReference>) -> Self {
        let qualifier = qualifier.into();
        DFSchema {
            fields: self
                .fields
                .into_iter()
                .map(|f| f.with_qualifier(qualifier.clone()))
                .collect(),
            ..self
        }
    }

    /// Get list of fully-qualified field names in this schema
    pub fn field_names(&self) -> Vec<String> {
        self.fields
            .iter()
            .map(|f| f.qualified_name())
            .collect::<Vec<_>>()
    }

    /// Get metadata of this schema
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Get functional dependencies
    pub fn functional_dependencies(&self) -> &FunctionalDependencies {
        &self.functional_dependencies
    }
}

impl From<DFSchema> for Schema {
    /// Convert DFSchema into a Schema
    fn from(df_schema: DFSchema) -> Self {
        let fields: Fields = df_schema
            .fields
            .into_iter()
            .map(|f| {
                Field::new(f.name, f.data_type.physical_type(), f.nullable)
                    .with_metadata(f.metadata)
            })
            .collect();
        Schema::new_with_metadata(fields, df_schema.metadata)
    }
}

impl From<&DFSchema> for Schema {
    /// Convert DFSchema reference into a Schema
    fn from(df_schema: &DFSchema) -> Self {
        let fields: Fields = df_schema
            .fields
            .iter()
            .map(|f| {
                Field::new(f.name(), f.data_type().physical_type(), f.is_nullable())
                    .with_metadata(f.metadata.clone())
            })
            .collect();
        Schema::new_with_metadata(fields, df_schema.metadata.clone())
    }
}

/// Create a `DFSchema` from an Arrow schema
impl TryFrom<Schema> for DFSchema {
    type Error = DataFusionError;
    fn try_from(schema: Schema) -> Result<Self, Self::Error> {
        Self::new_with_metadata(
            schema
                .fields()
                .iter()
                .map(|f| {
                    DFField::new_unqualified(
                        f.name(),
                        f.data_type().into(),
                        f.is_nullable(),
                    )
                    .with_metadata(f.metadata().clone())
                })
                .collect(),
            schema.metadata().clone(),
        )
    }
}

impl From<DFSchema> for SchemaRef {
    fn from(df_schema: DFSchema) -> Self {
        SchemaRef::new(df_schema.into())
    }
}

// Hashing refers to a subset of fields considered in PartialEq.
impl Hash for DFSchema {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.fields.hash(state);
        self.metadata.len().hash(state); // HashMap is not hashable
    }
}

/// Convenience trait to convert Schema like things to DFSchema and DFSchemaRef with fewer keystrokes
pub trait ToDFSchema
where
    Self: Sized,
{
    /// Attempt to create a DSSchema
    fn to_dfschema(self) -> Result<DFSchema>;

    /// Attempt to create a DSSchemaRef
    fn to_dfschema_ref(self) -> Result<DFSchemaRef> {
        Ok(Arc::new(self.to_dfschema()?))
    }
}

impl ToDFSchema for Schema {
    fn to_dfschema(self) -> Result<DFSchema> {
        DFSchema::try_from(self)
    }
}

impl ToDFSchema for SchemaRef {
    fn to_dfschema(self) -> Result<DFSchema> {
        // Attempt to use the Schema directly if there are no other
        // references, otherwise clone
        match Self::try_unwrap(self) {
            Ok(schema) => DFSchema::try_from(schema),
            Err(schemaref) => DFSchema::try_from(schemaref.as_ref().clone()),
        }
    }
}

impl ToDFSchema for Vec<DFField> {
    fn to_dfschema(self) -> Result<DFSchema> {
        DFSchema::new_with_metadata(self, HashMap::new())
    }
}

impl Display for DFSchema {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "fields:[{}], metadata:{:?}",
            self.fields
                .iter()
                .map(|field| field.qualified_name())
                .collect::<Vec<String>>()
                .join(", "),
            self.metadata
        )
    }
}

/// Provides schema information needed by certain methods of `Expr`
/// (defined in the datafusion-common crate).
///
/// Note that this trait is implemented for &[DFSchema] which is
/// widely used in the DataFusion codebase.
pub trait ExprSchema: std::fmt::Debug {
    /// Is this column reference nullable?
    fn nullable(&self, col: &Column) -> Result<bool>;

    /// What is the datatype of this column?
    fn data_type(&self, col: &Column) -> Result<&LogicalType>;

    /// Returns the column's optional metadata.
    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>>;
}

// Implement `ExprSchema` for `Arc<DFSchema>`
impl<P: AsRef<DFSchema> + std::fmt::Debug> ExprSchema for P {
    fn nullable(&self, col: &Column) -> Result<bool> {
        self.as_ref().nullable(col)
    }

    fn data_type(&self, col: &Column) -> Result<&LogicalType> {
        self.as_ref().data_type(col)
    }

    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>> {
        ExprSchema::metadata(self.as_ref(), col)
    }
}

impl ExprSchema for DFSchema {
    fn nullable(&self, col: &Column) -> Result<bool> {
        Ok(self.field_from_column(col)?.is_nullable())
    }

    fn data_type(&self, col: &Column) -> Result<&LogicalType> {
        Ok(self.field_from_column(col)?.data_type())
    }

    fn metadata(&self, col: &Column) -> Result<&HashMap<String, String>> {
        Ok(self.field_from_column(col)?.metadata())
    }
}

/// DFField wraps an Arrow field and adds an optional qualifier
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFField {
    /// Optional qualifier (usually a table or relation name)
    qualifier: Option<OwnedTableReference>,
    /// Arrow field definition
    // field: FieldRef,
    name: String,
    data_type: LogicalType,
    nullable: bool,
    /// A map of key-value pairs containing additional custom meta data.
    metadata: HashMap<String, String>,
}

impl Hash for DFField {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.qualifier.hash(state);
        self.name.hash(state);
        self.data_type.hash(state);
        self.nullable.hash(state);

        // ensure deterministic key order
        let mut keys: Vec<&String> = self.metadata.keys().collect();
        keys.sort();
        for k in keys {
            k.hash(state);
            self.metadata.get(k).expect("key valid").hash(state);
        }
    }
}

impl DFField {
    /// Creates a new `DFField`
    pub fn new<R: Into<OwnedTableReference>>(
        qualifier: Option<R>,
        name: &str,
        data_type: LogicalType,
        nullable: bool,
    ) -> Self {
        DFField {
            qualifier: qualifier.map(|s| s.into()),
            name: name.to_string(),
            data_type,
            nullable,
            metadata: Default::default(),
        }
    }

    /// Convenience method for creating new `DFField` without a qualifier
    pub fn new_unqualified(name: &str, data_type: LogicalType, nullable: bool) -> Self {
        DFField {
            qualifier: None,
            name: name.to_string(),
            data_type,
            nullable,
            metadata: Default::default(),
        }
    }

    /// Create a qualified field from an existing Arrow field
    pub fn from_qualified<'a>(
        qualifier: impl Into<TableReference<'a>>,
        field: impl Into<FieldRef>,
    ) -> Self {
        let field: FieldRef = field.into();
        Self::new(
            Some(qualifier.into().to_owned_reference()),
            field.name(),
            field.data_type().into(),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone())
    }

    /// Returns an immutable reference to the `DFField`'s unqualified name
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Returns an immutable reference to the `DFField`'s data-type
    pub fn data_type(&self) -> &LogicalType {
        &self.data_type
    }

    /// Indicates whether this `DFField` supports null values
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Returns a string to the `DFField`'s qualified name
    pub fn qualified_name(&self) -> String {
        if let Some(qualifier) = &self.qualifier {
            format!("{}.{}", qualifier, self.name)
        } else {
            self.name.clone()
        }
    }

    /// Builds a qualified column based on self
    pub fn qualified_column(&self) -> Column {
        Column {
            relation: self.qualifier.clone(),
            name: self.name.clone(),
        }
    }

    /// Builds an unqualified column based on self
    pub fn unqualified_column(&self) -> Column {
        Column {
            relation: None,
            name: self.name.clone(),
        }
    }

    /// Get the optional qualifier
    pub fn qualifier(&self) -> Option<&OwnedTableReference> {
        self.qualifier.as_ref()
    }

    /// Return field with qualifier stripped
    pub fn strip_qualifier(mut self) -> Self {
        self.qualifier = None;
        self
    }

    /// Return field with nullable specified
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Return field with new metadata
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Return field with new qualifier
    pub fn with_qualifier(mut self, qualifier: impl Into<OwnedTableReference>) -> Self {
        self.qualifier = Some(qualifier.into());
        self
    }
}

impl From<FieldRef> for DFField {
    fn from(value: FieldRef) -> Self {
        Self::new_unqualified(value.name(), value.data_type().into(), value.is_nullable())
            .with_metadata(value.metadata().clone())
    }
}

impl From<Field> for DFField {
    fn from(value: Field) -> Self {
        Self::from(Arc::new(value))
    }
}

/// DataFusion-specific extensions to [`Schema`].
pub trait SchemaExt {
    /// This is a specialized version of Eq that ignores differences
    /// in nullability and metadata.
    ///
    /// It works the same as [`DFSchema::equivalent_names_and_types`].
    fn equivalent_names_and_types(&self, other: &Self) -> bool;

    /// Returns true if the two schemas have the same qualified named
    /// fields with logically equivalent data types. Returns false otherwise.
    ///
    /// Use [DFSchema]::equivalent_names_and_types for stricter semantic type
    /// equivalence checking.
    fn logically_equivalent_names_and_types(&self, other: &Self) -> bool;
}

impl SchemaExt for Schema {
    fn equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }

        self.fields()
            .iter()
            .zip(other.fields().iter())
            .all(|(f1, f2)| {
                f1.name() == f2.name()
                    && DFSchema::datatype_is_semantically_equal(
                        f1.data_type(),
                        f2.data_type(),
                    )
            })
    }

    fn logically_equivalent_names_and_types(&self, other: &Self) -> bool {
        if self.fields().len() != other.fields().len() {
            return false;
        }

        self.fields()
            .iter()
            .zip(other.fields().iter())
            .all(|(f1, f2)| {
                f1.name() == f2.name()
                    && DFSchema::datatype_is_semantically_equal(
                        f1.data_type(),
                        f2.data_type(),
                    )
            })
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_contains;

    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn qualifier_in_name() -> Result<()> {
        let col = Column::from_name("t1.c0");
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Schema error: No field named \"t1.c0\". Valid fields are t1.c0, t1.c1."
        );
        Ok(())
    }

    #[test]
    fn quoted_qualifiers_in_name() -> Result<()> {
        let col = Column::from_name("t1.c0");
        let schema = DFSchema::try_from_qualified_schema(
            "t1",
            &Schema::new(vec![
                Field::new("CapitalColumn", DataType::Boolean, true),
                Field::new("field.with.period", DataType::Boolean, true),
            ]),
        )?;

        // lookup with unqualified name "t1.c0"
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Schema error: No field named \"t1.c0\". Valid fields are t1.\"CapitalColumn\", t1.\"field.with.period\"."
        );
        Ok(())
    }

    #[test]
    fn from_unqualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from(field);
        assert_eq!("c0", field.name());
        assert_eq!("c0", field.qualified_name());
    }

    #[test]
    fn from_qualified_field() {
        let field = Field::new("c0", DataType::Boolean, true);
        let field = DFField::from_qualified("t1", field);
        assert_eq!("c0", field.name());
        assert_eq!("t1.c0", field.qualified_name());
    }

    #[test]
    fn from_unqualified_schema() -> Result<()> {
        let schema = DFSchema::try_from(test_schema_1())?;
        assert_eq!("fields:[c0, c1], metadata:{}", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        assert_eq!("fields:[t1.c0, t1.c1], metadata:{}", schema.to_string());
        Ok(())
    }

    #[test]
    fn from_qualified_schema_into_arrow_schema() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let arrow_schema: Schema = schema.into();
        let expected = "Field { name: \"c0\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, \
        Field { name: \"c1\", data_type: Boolean, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }";
        assert_eq!(expected, arrow_schema.to_string());
        Ok(())
    }

    #[test]
    fn join_qualified() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from_qualified_schema("t2", &test_schema_1())?;
        let join = left.join(&right)?;
        assert_eq!(
            "fields:[t1.c0, t1.c1, t2.c0, t2.c1], metadata:{}",
            join.to_string()
        );
        // test valid access
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t1"), "c0")
            .is_ok());
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t2"), "c0")
            .is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t2.c0").is_err());
        Ok(())
    }

    #[test]
    fn join_qualified_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let join = left.join(&right);
        assert!(join.err().is_none());
        Ok(())
    }

    #[test]
    fn join_unqualified_duplicate() -> Result<()> {
        let left = DFSchema::try_from(test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert_eq!(
            join.unwrap_err().strip_backtrace(),
            "Schema error: Schema contains duplicate unqualified field name c0"
        );
        Ok(())
    }

    #[test]
    fn join_mixed() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_2())?;
        let join = left.join(&right)?;
        assert_eq!(
            "fields:[t1.c0, t1.c1, c100, c101], metadata:{}",
            join.to_string()
        );
        // test valid access
        assert!(join
            .field_with_qualified_name(&TableReference::bare("t1"), "c0")
            .is_ok());
        assert!(join.field_with_unqualified_name("c0").is_ok());
        assert!(join.field_with_unqualified_name("c100").is_ok());
        assert!(join.field_with_name(None, "c100").is_ok());
        // test invalid access
        assert!(join.field_with_unqualified_name("t1.c0").is_err());
        assert!(join.field_with_unqualified_name("t1.c100").is_err());
        assert!(join
            .field_with_qualified_name(&TableReference::bare(""), "c100")
            .is_err());
        Ok(())
    }

    #[test]
    fn join_mixed_duplicate() -> Result<()> {
        let left = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let right = DFSchema::try_from(test_schema_1())?;
        let join = left.join(&right);
        assert_contains!(join.unwrap_err().to_string(),
                         "Schema error: Schema contains qualified \
                          field name t1.c0 and unqualified field name c0 which would be ambiguous");
        Ok(())
    }

    #[allow(deprecated)]
    #[test]
    fn helpful_error_messages() -> Result<()> {
        let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
        let expected_help = "Valid fields are t1.c0, t1.c1.";
        // Pertinent message parts
        let expected_err_msg = "Fully qualified field name 't1.c0'";
        assert_contains!(
            schema
                .field_with_qualified_name(&TableReference::bare("x"), "y")
                .unwrap_err()
                .to_string(),
            expected_help
        );
        assert_contains!(
            schema
                .field_with_unqualified_name("y")
                .unwrap_err()
                .to_string(),
            expected_help
        );
        assert_contains!(schema.index_of("y").unwrap_err().to_string(), expected_help);
        assert_contains!(
            schema.index_of("t1.c0").unwrap_err().to_string(),
            expected_err_msg
        );
        Ok(())
    }

    #[test]
    fn select_without_valid_fields() {
        let schema = DFSchema::empty();

        let col = Column::from_qualified_name("t1.c0");
        let err = schema.index_of_column(&col).unwrap_err();
        assert_eq!(err.strip_backtrace(), "Schema error: No field named t1.c0.");

        // the same check without qualifier
        let col = Column::from_name("c0");
        let err = schema.index_of_column(&col).err().unwrap();
        assert_eq!(err.strip_backtrace(), "Schema error: No field named c0.");
    }

    #[test]
    fn equivalent_names_and_types() {
        let arrow_field1 = Field::new("f1", DataType::Int16, true);
        let arrow_field1_meta = arrow_field1.clone().with_metadata(test_metadata_n(2));

        let field1_i16_t = DFField::from(arrow_field1);
        let field1_i16_t_meta = DFField::from(arrow_field1_meta);
        let field1_i16_t_qualified =
            DFField::from_qualified("foo", field1_i16_t.field().clone());
        let field1_i16_f = DFField::from(Field::new("f1", DataType::Int16, false));
        let field1_i32_t = DFField::from(Field::new("f1", DataType::Int32, true));
        let field2_i16_t = DFField::from(Field::new("f2", DataType::Int16, true));
        let field3_i16_t = DFField::from(Field::new("f3", DataType::Int16, true));

        let dict =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let field_dict_t = DFField::from(Field::new("f_dict", dict.clone(), true));
        let field_dict_f = DFField::from(Field::new("f_dict", dict, false));

        let list_t = DFField::from(Field::new_list(
            "f_list",
            field1_i16_t.field().clone(),
            true,
        ));
        let list_f = DFField::from(Field::new_list(
            "f_list",
            field1_i16_f.field().clone(),
            false,
        ));

        let list_f_name = DFField::from(Field::new_list(
            "f_list",
            field2_i16_t.field().clone(),
            false,
        ));

        let struct_t = DFField::from(Field::new_struct(
            "f_struct",
            vec![field1_i16_t.field().clone()],
            true,
        ));
        let struct_f = DFField::from(Field::new_struct(
            "f_struct",
            vec![field1_i16_f.field().clone()],
            false,
        ));

        let struct_f_meta = DFField::from(Field::new_struct(
            "f_struct",
            vec![field1_i16_t_meta.field().clone()],
            false,
        ));

        let struct_f_type = DFField::from(Field::new_struct(
            "f_struct",
            vec![field1_i32_t.field().clone()],
            false,
        ));

        // same
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_t],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // same but metadata is different, should still be true
        TestCase {
            fields1: vec![&field1_i16_t_meta],
            fields2: vec![&field1_i16_t],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // different name
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field2_i16_t],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // different type
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i32_t],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // different nullability
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_f],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // different qualifier
        TestCase {
            fields1: vec![&field1_i16_t],
            fields2: vec![&field1_i16_t_qualified],
            expected_dfschema: false,
            expected_arrow: true,
        }
        .run();

        // different name after first
        TestCase {
            fields1: vec![&field2_i16_t, &field1_i16_t],
            fields2: vec![&field2_i16_t, &field3_i16_t],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // different number
        TestCase {
            fields1: vec![&field1_i16_t, &field2_i16_t],
            fields2: vec![&field1_i16_t],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // dictionary
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field_dict_t],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // dictionary (different nullable)
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field_dict_f],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // dictionary (wrong type)
        TestCase {
            fields1: vec![&field_dict_t],
            fields2: vec![&field1_i16_t],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // list (different embedded nullability)
        TestCase {
            fields1: vec![&list_t],
            fields2: vec![&list_f],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // list (different sub field names)
        TestCase {
            fields1: vec![&list_t],
            fields2: vec![&list_f_name],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        // struct
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // struct (different embedded meta)
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f_meta],
            expected_dfschema: true,
            expected_arrow: true,
        }
        .run();

        // struct (different field type)
        TestCase {
            fields1: vec![&struct_t],
            fields2: vec![&struct_f_type],
            expected_dfschema: false,
            expected_arrow: false,
        }
        .run();

        #[derive(Debug)]
        struct TestCase<'a> {
            fields1: Vec<&'a DFField>,
            fields2: Vec<&'a DFField>,
            expected_dfschema: bool,
            expected_arrow: bool,
        }

        impl<'a> TestCase<'a> {
            fn run(self) {
                println!("Running {self:#?}");
                let schema1 = to_df_schema(self.fields1);
                let schema2 = to_df_schema(self.fields2);
                assert_eq!(
                    schema1.equivalent_names_and_types(&schema2),
                    self.expected_dfschema,
                    "Comparison did not match expected: {}\n\n\
                     schema1:\n\n{:#?}\n\nschema2:\n\n{:#?}",
                    self.expected_dfschema,
                    schema1,
                    schema2
                );

                let arrow_schema1 = Schema::from(schema1);
                let arrow_schema2 = Schema::from(schema2);
                assert_eq!(
                    arrow_schema1.equivalent_names_and_types(&arrow_schema2),
                    self.expected_arrow,
                    "Comparison did not match expected: {}\n\n\
                     arrow schema1:\n\n{:#?}\n\n arrow schema2:\n\n{:#?}",
                    self.expected_arrow,
                    arrow_schema1,
                    arrow_schema2
                );
            }
        }

        fn to_df_schema(fields: Vec<&DFField>) -> DFSchema {
            let fields = fields.into_iter().cloned().collect();
            DFSchema::new_with_metadata(fields, HashMap::new()).unwrap()
        }
    }

    #[test]
    fn into() {
        // Demonstrate how to convert back and forth between Schema, SchemaRef, DFSchema, and DFSchemaRef
        let metadata = test_metadata();
        let arrow_schema = Schema::new_with_metadata(
            vec![Field::new("c0", DataType::Int64, true)],
            metadata.clone(),
        );
        let arrow_schema_ref = Arc::new(arrow_schema.clone());

        let df_schema = DFSchema::new_with_metadata(
            vec![DFField::new_unqualified("c0", DataType::Int64, true)],
            metadata,
        )
        .unwrap();
        let df_schema_ref = Arc::new(df_schema.clone());

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = arrow_schema_ref.clone();

            assert_eq!(df_schema, arrow_schema.to_dfschema().unwrap());
            assert_eq!(df_schema, arrow_schema_ref.to_dfschema().unwrap());
        }

        {
            let arrow_schema = arrow_schema.clone();
            let arrow_schema_ref = arrow_schema_ref.clone();

            assert_eq!(df_schema_ref, arrow_schema.to_dfschema_ref().unwrap());
            assert_eq!(df_schema_ref, arrow_schema_ref.to_dfschema_ref().unwrap());
        }

        // Now, consume the refs
        assert_eq!(df_schema_ref, arrow_schema.to_dfschema_ref().unwrap());
        assert_eq!(df_schema_ref, arrow_schema_ref.to_dfschema_ref().unwrap());
    }

    fn test_schema_1() -> Schema {
        Schema::new(vec![
            Field::new("c0", DataType::Boolean, true),
            Field::new("c1", DataType::Boolean, true),
        ])
    }
    #[test]
    fn test_dfschema_to_schema_convertion() {
        let mut a_metadata = HashMap::new();
        a_metadata.insert("key".to_string(), "value".to_string());
        let a_field = Field::new("a", DataType::Int64, false).with_metadata(a_metadata);

        let mut b_metadata = HashMap::new();
        b_metadata.insert("key".to_string(), "value".to_string());
        let b_field = Field::new("b", DataType::Int64, false).with_metadata(b_metadata);

        let a: DFField = DFField::from_qualified("table1", a_field);
        let b: DFField = DFField::from_qualified("table1", b_field);

        let df_schema = Arc::new(
            DFSchema::new_with_metadata([a, b].to_vec(), HashMap::new()).unwrap(),
        );
        let schema: Schema = df_schema.as_ref().clone().into();
        let a_df = df_schema.fields.get(0).unwrap().field();
        let a_arrow = schema.fields.get(0).unwrap();
        assert_eq!(a_df.metadata(), a_arrow.metadata())
    }

    #[test]
    fn test_contain_column() -> Result<()> {
        // qualified exists
        {
            let col = Column::from_qualified_name("t1.c0");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(schema.is_column_from_schema(&col)?);
        }

        // qualified not exists
        {
            let col = Column::from_qualified_name("t1.c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col)?);
        }

        // unqualified exists
        {
            let col = Column::from_name("c0");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(schema.is_column_from_schema(&col)?);
        }

        // unqualified not exists
        {
            let col = Column::from_name("c2");
            let schema = DFSchema::try_from_qualified_schema("t1", &test_schema_1())?;
            assert!(!schema.is_column_from_schema(&col)?);
        }

        Ok(())
    }

    fn test_schema_2() -> Schema {
        Schema::new(vec![
            Field::new("c100", DataType::Boolean, true),
            Field::new("c101", DataType::Boolean, true),
        ])
    }

    fn test_metadata() -> HashMap<String, String> {
        test_metadata_n(2)
    }

    fn test_metadata_n(n: usize) -> HashMap<String, String> {
        (0..n).map(|i| (format!("k{i}"), format!("v{i}"))).collect()
    }
}
