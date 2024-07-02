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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};

use super::field::{LogicalField, LogicalFieldRef};
use super::fields::LogicalFields;

#[derive(Debug, Default)]
pub struct LogicalSchemaBuilder {
    fields: Vec<LogicalFieldRef>,
    metadata: HashMap<String, String>,
}

impl LogicalSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            fields: Vec::with_capacity(capacity),
            metadata: Default::default(),
        }
    }

    pub fn push(&mut self, field: impl Into<LogicalFieldRef>) {
        self.fields.push(field.into())
    }

    pub fn remove(&mut self, idx: usize) -> LogicalFieldRef {
        self.fields.remove(idx)
    }

    pub fn field(&mut self, idx: usize) -> &LogicalFieldRef {
        &mut self.fields[idx]
    }

    pub fn field_mut(&mut self, idx: usize) -> &mut LogicalFieldRef {
        &mut self.fields[idx]
    }

    pub fn metadata(&mut self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    pub fn reverse(&mut self) {
        self.fields.reverse();
    }

    pub fn finish(self) -> LogicalSchema {
        LogicalSchema {
            fields: self.fields.into(),
            metadata: self.metadata,
        }
    }
}

impl From<&LogicalFields> for LogicalSchemaBuilder {
    fn from(value: &LogicalFields) -> Self {
        Self {
            fields: value.to_vec(),
            metadata: Default::default(),
        }
    }
}

impl From<LogicalFields> for LogicalSchemaBuilder {
    fn from(value: LogicalFields) -> Self {
        Self {
            fields: value.to_vec(),
            metadata: Default::default(),
        }
    }
}

impl From<&LogicalSchema> for LogicalSchemaBuilder {
    fn from(value: &LogicalSchema) -> Self {
        Self::from(value.clone())
    }
}

impl From<LogicalSchema> for LogicalSchemaBuilder {
    fn from(value: LogicalSchema) -> Self {
        Self {
            fields: value.fields.to_vec(),
            metadata: value.metadata,
        }
    }
}

impl Extend<LogicalFieldRef> for LogicalSchemaBuilder {
    fn extend<T: IntoIterator<Item=LogicalFieldRef>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.fields.reserve(iter.size_hint().0);
        for f in iter {
            self.push(f)
        }
    }
}

impl Extend<LogicalField> for LogicalSchemaBuilder {
    fn extend<T: IntoIterator<Item=LogicalField>>(&mut self, iter: T) {
        let iter = iter.into_iter();
        self.fields.reserve(iter.size_hint().0);
        for f in iter {
            self.push(f)
        }
    }
}

pub type LogicalSchemaRef = Arc<LogicalSchema>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSchema {
    pub fields: LogicalFields,
    pub metadata: HashMap<String, String>,
}

impl From<Schema> for LogicalSchema {
    fn from(value: Schema) -> Self {
        Self {
            fields: value.fields.into(),
            metadata: value.metadata,
        }
    }
}

impl From<&Schema> for LogicalSchema {
    fn from(value: &Schema) -> Self {
        Self::from(value.clone())
    }
}

impl From<SchemaRef> for LogicalSchema {
    fn from(value: SchemaRef) -> Self {
        Self::from(value.as_ref())
    }
}

impl From<&SchemaRef> for LogicalSchema {
    fn from(value: &SchemaRef) -> Self {
        Self::from(value.as_ref())
    }
}

impl Into<Schema> for LogicalSchema {
    fn into(self) -> Schema {
        Schema {
            fields: self.fields.into(),
            metadata: self.metadata,
        }
    }
}


impl LogicalSchema {
    pub fn new(fields: impl Into<LogicalFields>) -> Self {
        Self::new_with_metadata(fields, HashMap::new())
    }

    #[inline]
    pub fn new_with_metadata(fields: impl Into<LogicalFields>, metadata: HashMap<String, String>) -> Self {
        Self {
            fields: fields.into(),
            metadata,
        }
    }

    #[inline]
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    pub fn field(&self, i: usize) -> &LogicalFieldRef {
        &self.fields[i]
    }

    pub fn fields(&self) -> &LogicalFields {
        &self.fields
    }
}
