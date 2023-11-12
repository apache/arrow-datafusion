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
use std::{
    fmt::{self, Display},
    hash::{Hash, Hasher},
};

use crate::{Expr, LogicalPlan};

use datafusion_common::logical_type::LogicalType;
use datafusion_common::parsers::CompressionTypeVariant;
use datafusion_common::{
    Constraints, DFSchemaRef, OwnedSchemaReference, OwnedTableReference,
};

/// Various types of DDL  (CREATE / DROP) catalog manipulation
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum DdlStatement {
    /// Creates an external table.
    CreateExternalTable(CreateExternalTable),
    /// Creates an in memory table.
    CreateMemoryTable(CreateMemoryTable),
    /// Creates a new view.
    CreateView(CreateView),
    /// Creates a new catalog schema.
    CreateCatalogSchema(CreateCatalogSchema),
    /// Creates a new catalog (aka "Database").
    CreateCatalog(CreateCatalog),
    /// Creates a new user defined data type.
    CreateType(CreateType),
    /// Drops a table.
    DropTable(DropTable),
    /// Drops a view.
    DropView(DropView),
    /// Drops a catalog schema
    DropCatalogSchema(DropCatalogSchema),
}

impl DdlStatement {
    /// Get a reference to the logical plan's schema
    pub fn schema(&self) -> &DFSchemaRef {
        match self {
            DdlStatement::CreateExternalTable(CreateExternalTable { schema, .. }) => {
                schema
            }
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. })
            | DdlStatement::CreateView(CreateView { input, .. }) => input.schema(),
            DdlStatement::CreateCatalogSchema(CreateCatalogSchema { schema, .. }) => {
                schema
            }
            DdlStatement::CreateCatalog(CreateCatalog { schema, .. }) => schema,
            DdlStatement::CreateType(CreateType { schema, .. }) => schema,
            DdlStatement::DropTable(DropTable { schema, .. }) => schema,
            DdlStatement::DropView(DropView { schema, .. }) => schema,
            DdlStatement::DropCatalogSchema(DropCatalogSchema { schema, .. }) => schema,
        }
    }

    /// Return a descriptive string describing the type of this
    /// [`DdlStatement`]
    pub fn name(&self) -> &str {
        match self {
            DdlStatement::CreateExternalTable(_) => "CreateExternalTable",
            DdlStatement::CreateMemoryTable(_) => "CreateMemoryTable",
            DdlStatement::CreateView(_) => "CreateView",
            DdlStatement::CreateCatalogSchema(_) => "CreateCatalogSchema",
            DdlStatement::CreateCatalog(_) => "CreateCatalog",
            DdlStatement::CreateType(_) => "CreateType",
            DdlStatement::DropTable(_) => "DropTable",
            DdlStatement::DropView(_) => "DropView",
            DdlStatement::DropCatalogSchema(_) => "DropCatalogSchema",
        }
    }

    /// Return all inputs for this plan
    pub fn inputs(&self) -> Vec<&LogicalPlan> {
        match self {
            DdlStatement::CreateExternalTable(_) => vec![],
            DdlStatement::CreateCatalogSchema(_) => vec![],
            DdlStatement::CreateCatalog(_) => vec![],
            DdlStatement::CreateMemoryTable(CreateMemoryTable { input, .. }) => {
                vec![input]
            }
            DdlStatement::CreateView(CreateView { input, .. }) => vec![input],
            DdlStatement::CreateType(_) => vec![],
            DdlStatement::DropTable(_) => vec![],
            DdlStatement::DropView(_) => vec![],
            DdlStatement::DropCatalogSchema(_) => vec![],
        }
    }

    /// Return a `format`able structure with the a human readable
    /// description of this LogicalPlan node per node, not including
    /// children.
    ///
    /// See [crate::LogicalPlan::display] for an example
    pub fn display(&self) -> impl fmt::Display + '_ {
        struct Wrapper<'a>(&'a DdlStatement);
        impl<'a> Display for Wrapper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self.0 {
                    DdlStatement::CreateExternalTable(CreateExternalTable {
                        ref name,
                        constraints,
                        ..
                    }) => {
                        write!(f, "CreateExternalTable: {name:?}{constraints}")
                    }
                    DdlStatement::CreateMemoryTable(CreateMemoryTable {
                        name,
                        constraints,
                        ..
                    }) => {
                        write!(f, "CreateMemoryTable: {name:?}{constraints}")
                    }
                    DdlStatement::CreateView(CreateView { name, .. }) => {
                        write!(f, "CreateView: {name:?}")
                    }
                    DdlStatement::CreateCatalogSchema(CreateCatalogSchema {
                        schema_name,
                        ..
                    }) => {
                        write!(f, "CreateCatalogSchema: {schema_name:?}")
                    }
                    DdlStatement::CreateCatalog(CreateCatalog {
                        catalog_name, ..
                    }) => {
                        write!(f, "CreateCatalog: {catalog_name:?}")
                    }
                    DdlStatement::CreateType(CreateType { name, .. }) => {
                        write!(f, "CreateType: {name}")
                    }
                    DdlStatement::DropTable(DropTable {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropTable: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropView(DropView {
                        name, if_exists, ..
                    }) => {
                        write!(f, "DropView: {name:?} if not exist:={if_exists}")
                    }
                    DdlStatement::DropCatalogSchema(DropCatalogSchema {
                        name,
                        if_exists,
                        cascade,
                        ..
                    }) => {
                        write!(f, "DropCatalogSchema: {name:?} if not exist:={if_exists} cascade:={cascade}")
                    }
                }
            }
        }
        Wrapper(self)
    }
}

/// Creates an external table.
#[derive(Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// The table schema
    pub schema: DFSchemaRef,
    /// The table name
    pub name: OwnedTableReference,
    /// The physical location
    pub location: String,
    /// The file type of physical file
    pub file_type: String,
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// SQL used to create the table, if available
    pub definition: Option<String>,
    /// Order expressions supplied by user
    pub order_exprs: Vec<Vec<Expr>>,
    /// File compression type (GZIP, BZIP2, XZ, ZSTD)
    pub file_compression_type: CompressionTypeVariant,
    /// Whether the table is an infinite streams
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: HashMap<String, String>,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
}

// Hashing refers to a subset of fields considered in PartialEq.
impl Hash for CreateExternalTable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
        self.location.hash(state);
        self.file_type.hash(state);
        self.has_header.hash(state);
        self.delimiter.hash(state);
        self.table_partition_cols.hash(state);
        self.if_not_exists.hash(state);
        self.definition.hash(state);
        self.file_compression_type.hash(state);
        self.order_exprs.hash(state);
        self.unbounded.hash(state);
        self.options.len().hash(state); // HashMap is not hashable
    }
}

/// Creates an in memory table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateMemoryTable {
    /// The table name
    pub name: OwnedTableReference,
    /// The list of constraints in the schema, such as primary key, unique, etc.
    pub constraints: Constraints,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
}

/// Creates a view.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateView {
    /// The table name
    pub name: OwnedTableReference,
    /// The logical plan
    pub input: Arc<LogicalPlan>,
    /// Option to not error if table already exists
    pub or_replace: bool,
    /// SQL used to create the view, if available
    pub definition: Option<String>,
}

/// Creates a catalog (aka "Database").
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalog {
    /// The catalog name
    pub catalog_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Creates a schema.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateCatalogSchema {
    /// The table schema
    pub schema_name: String,
    /// Do nothing (except issuing a notice) if a schema with the same name already exists
    pub if_not_exists: bool,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Creates a schema.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CreateType {
    pub name: String,
    pub data_type: LogicalType,
    /// Empty schema
    pub schema: DFSchemaRef,
}

/// Drops a table.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DropTable {
    /// The table name
    pub name: OwnedTableReference,
    /// If the table exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

/// Drops a view.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DropView {
    /// The view name
    pub name: OwnedTableReference,
    /// If the view exists
    pub if_exists: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}

/// Drops a schema
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DropCatalogSchema {
    /// The schema name
    pub name: OwnedSchemaReference,
    /// If the schema exists
    pub if_exists: bool,
    /// Whether drop should cascade
    pub cascade: bool,
    /// Dummy schema
    pub schema: DFSchemaRef,
}
