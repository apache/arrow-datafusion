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

use std::ops::Deref;
use std::sync::Arc;

use arrow_schema::{Field, Fields, UnionFields};

use super::field::{LogicalField, LogicalFieldRef};

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct LogicalFields(Arc<[LogicalFieldRef]>);

impl std::fmt::Debug for LogicalFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl From<&Fields> for LogicalFields {
    fn from(value: &Fields) -> Self {
        Self(value.iter().map(|v| LogicalFieldRef::new(v.into())).collect())
    }
}

impl From<Fields> for LogicalFields {
    fn from(value: Fields) -> Self {
        Self::from(&value)
    }
}

impl Into<Fields> for LogicalFields {
    fn into(self) -> Fields {
        Fields::from(
            self.iter()
                .map(|f| f.as_ref().clone().into())
                .collect::<Vec<Field>>()
        )
    }
}

impl Default for LogicalFields {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<LogicalField> for LogicalFields {
    fn from_iter<T: IntoIterator<Item=LogicalField>>(iter: T) -> Self {
        iter.into_iter().map(Arc::new).collect()
    }
}

impl FromIterator<LogicalFieldRef> for LogicalFields {
    fn from_iter<T: IntoIterator<Item=LogicalFieldRef>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<Vec<LogicalField>> for LogicalFields {
    fn from(value: Vec<LogicalField>) -> Self {
        value.into_iter().collect()
    }
}

impl From<Vec<LogicalFieldRef>> for LogicalFields {
    fn from(value: Vec<LogicalFieldRef>) -> Self {
        Self(value.into())
    }
}

impl From<&[LogicalFieldRef]> for LogicalFields {
    fn from(value: &[LogicalFieldRef]) -> Self {
        Self(value.into())
    }
}

impl<const N: usize> From<[LogicalFieldRef; N]> for LogicalFields {
    fn from(value: [LogicalFieldRef; N]) -> Self {
        Self(Arc::new(value))
    }
}

impl Deref for LogicalFields {
    type Target = [LogicalFieldRef];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a> IntoIterator for &'a LogicalFields {
    type Item = &'a LogicalFieldRef;
    type IntoIter = std::slice::Iter<'a, LogicalFieldRef>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl LogicalFields {
    pub fn empty() -> Self {
        Self(Arc::new([]))
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct LogicalUnionFields(Arc<[(i8, LogicalFieldRef)]>);

impl std::fmt::Debug for LogicalUnionFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.as_ref().fmt(f)
    }
}

impl FromIterator<(i8, LogicalFieldRef)> for LogicalUnionFields {
    fn from_iter<T: IntoIterator<Item = (i8, LogicalFieldRef)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl From<&UnionFields> for LogicalUnionFields {
    fn from(value: &UnionFields) -> Self {
        Self::from_iter(value.iter().map(|(i, f)| (i, LogicalFieldRef::new(f.into()))))
    }
}

impl From<UnionFields> for LogicalUnionFields {
    fn from(value: UnionFields) -> Self {
        Self::from(&value)
    }
}
