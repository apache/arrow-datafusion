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
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{DFSchemaRef, TableReference};
use datafusion_common::logical_type::field::LogicalField;
use datafusion_common::logical_type::schema::LogicalSchema;
use crate::LogicalPlan;

/// Operator that copies the contents of a database to file(s)
#[derive(Clone)]
pub struct CopyTo {
    /// The relation that determines the tuples to write to the output file(s)
    pub input: Arc<LogicalPlan>,
    /// The location to write the file(s)
    pub output_url: String,
    /// Determines which, if any, columns should be used for hive-style partitioned writes
    pub partition_by: Vec<String>,
    /// File type trait
    pub file_type: Arc<dyn FileType>,
    /// SQL Options that can affect the formats
    pub options: HashMap<String, String>,
}

// Implement PartialEq manually
impl PartialEq for CopyTo {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.output_url == other.output_url
    }
}

// Implement Eq (no need for additional logic over PartialEq)
impl Eq for CopyTo {}

// Implement Hash manually
impl Hash for CopyTo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
        self.output_url.hash(state);
    }
}

/// The operator that modifies the content of a database (adapted from
/// substrait WriteRel)
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DmlStatement {
    /// The table name
    pub table_name: TableReference,
    /// The schema of the table (must align with Rel input)
    pub table_schema: DFSchemaRef,
    /// The type of operation to perform
    pub op: WriteOp,
    /// The relation that determines the tuples to add/remove/modify the schema must match with table_schema
    pub input: Arc<LogicalPlan>,
    /// The schema of the output relation
    pub output_schema: DFSchemaRef,
}

impl DmlStatement {
    /// Creates a new DML statement with the output schema set to a single `count` column.
    pub fn new(
        table_name: TableReference,
        table_schema: DFSchemaRef,
        op: WriteOp,
        input: Arc<LogicalPlan>,
    ) -> Self {
        Self {
            table_name,
            table_schema,
            op,
            input,

            // The output schema is always a single column with the number of rows affected
            output_schema: make_count_schema(),
        }
    }

    /// Return a descriptive name of this [`DmlStatement`]
    pub fn name(&self) -> &str {
        self.op.name()
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum WriteOp {
    InsertOverwrite,
    InsertInto,
    Delete,
    Update,
    Ctas,
}

impl WriteOp {
    /// Return a descriptive name of this [`WriteOp`]
    pub fn name(&self) -> &str {
        match self {
            WriteOp::InsertOverwrite => "Insert Overwrite",
            WriteOp::InsertInto => "Insert Into",
            WriteOp::Delete => "Delete",
            WriteOp::Update => "Update",
            WriteOp::Ctas => "Ctas",
        }
    }
}

impl Display for WriteOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

fn make_count_schema() -> DFSchemaRef {
    Arc::new(
        LogicalSchema::new(vec![LogicalField::new("count", DataType::UInt64, false)]).try_into().unwrap()
    )
}
