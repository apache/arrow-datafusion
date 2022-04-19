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

//! Describes the interface and built-in implementations of catalogs,
//! representing collections of named schemas.

use crate::catalog::schema::SchemaProvider;
use crate::catalog::TableReference;
use crate::datasource::TableProvider;
use datafusion_common::{DataFusionError, Result};
use parking_lot::RwLock;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Represent a list of named catalogs
pub trait CatalogList: Sync + Send {
    /// Returns the catalog list as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Adds a new catalog to this catalog list
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>>;

    /// Retrieves the list of available catalog names
    fn catalog_names(&self) -> Vec<String>;

    /// Retrieves a specific catalog by name, provided it exists.
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>>;
}

/// Get a TableProvider from the catalog
pub fn get_table_provider(
    catalog_list: &dyn CatalogList,
    table_name: &str,
) -> Option<Arc<dyn TableProvider>> {
    // TODO do we have these defined as defaults somewhere?
    let mut catalog_name = "datafusion".to_owned();
    let mut schema_name = "public".to_owned();
    let table_ref_name;

    let table_ref: TableReference = table_name.into();
    match table_ref {
        TableReference::Bare { table } => table_ref_name = table.to_string(),
        TableReference::Partial { schema, table } => {
            schema_name = schema.to_string();
            table_ref_name = table.to_string();
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            catalog_name = catalog.to_string();
            schema_name = schema.to_string();
            table_ref_name = table.to_string();
        }
    }

    match catalog_list.catalog(&catalog_name) {
        Some(catalog) => match catalog.schema(&schema_name) {
            Some(schema) => match schema.table(&table_ref_name) {
                Some(table) => Some(table.clone()),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Simple in-memory list of catalogs
pub struct MemoryCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: RwLock<HashMap<String, Arc<dyn CatalogProvider>>>,
}

impl MemoryCatalogList {
    /// Instantiates a new `MemoryCatalogList` with an empty collection of catalogs
    pub fn new() -> Self {
        Self {
            catalogs: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemoryCatalogList {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogList for MemoryCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        let mut catalogs = self.catalogs.write();
        catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        let catalogs = self.catalogs.read();
        catalogs.keys().map(|s| s.to_string()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let catalogs = self.catalogs.read();
        catalogs.get(name).cloned()
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a catalog, comprising a number of named schemas.
pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`](std::any::Any)
    /// so that it can be downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available schema names in this catalog.
    fn schema_names(&self) -> Vec<String>;

    /// Retrieves a specific schema from the catalog by name, provided it exists.
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;

    /// Adds a new schema to this catalog.
    ///
    /// If a schema of the same name existed before, it is replaced in
    /// the catalog and returned.
    ///
    /// By default returns a "Not Implemented" error
    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // use variables to avoid unused variable warnings
        let _ = name;
        let _ = schema;
        Err(DataFusionError::NotImplemented(
            "Registering new schemas is not supported".to_string(),
        ))
    }
}

/// Simple in-memory implementation of a catalog.
pub struct MemoryCatalogProvider {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl MemoryCatalogProvider {
    /// Instantiates a new MemoryCatalogProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
        }
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read();
        schemas.get(name).cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write();
        Ok(schemas.insert(name.into(), schema))
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::schema::MemorySchemaProvider;

    use super::*;

    #[test]
    fn default_register_schema_not_supported() {
        // mimic a new CatalogProvider and ensure it does not support registering schemas
        struct TestProvider {}
        impl CatalogProvider for TestProvider {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn schema_names(&self) -> Vec<String> {
                unimplemented!()
            }

            fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
                unimplemented!()
            }
        }

        let schema = Arc::new(MemorySchemaProvider::new()) as _;
        let catalog = Arc::new(TestProvider {});

        match catalog.register_schema("foo", schema) {
            Ok(_) => panic!("unexpected OK"),
            Err(e) => assert_eq!(e.to_string(), "This feature is not implemented: Registering new schemas is not supported"),
        };
    }
}
