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

//! Describes the interface and built-in implementations of schemas,
//! representing collections of named tables.

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_common::{exec_err, DataFusionError};
use std::any::Any;
use std::sync::Arc;

use crate::datasource::TableProvider;
use crate::error::Result;

/// Represents a schema, comprising a number of named tables.
///
/// Please see [`CatalogProvider`] for details of implementing a custom catalog.
///
/// [`CatalogProvider`]: super::CatalogProvider
#[async_trait]
pub trait SchemaProvider: Sync + Send {
    /// Returns the owner of the Schema, default is None. This value is reported
    /// as part of `information_tables.schemata
    fn owner_name(&self) -> Option<&str> {
        None
    }

    /// Returns this `SchemaProvider` as [`Any`] so that it can be downcast to a
    /// specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Retrieves the list of available table names in this schema.
    fn table_names(&self) -> Vec<String>;

    /// Retrieves a specific table from the schema by name, if it exists,
    /// otherwise returns `None`.
    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;

    /// If supported by the implementation, adds a new table named `name` to
    /// this schema.
    ///
    /// If a table of the same name was already registered, returns "Table
    /// already exists" error.
    #[allow(unused_variables)]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support registering tables")
    }

    /// If supported by the implementation, removes the `name` table from this
    /// schema and returns the previously registered [`TableProvider`], if any.
    ///
    /// If no `name` table exists, returns Ok(None).
    #[allow(unused_variables)]
    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        exec_err!("schema provider does not support deregistering tables")
    }

    /// Returns true if table exist in the schema provider, false otherwise.
    fn table_exist(&self, name: &str) -> bool;
}

/// Simple in-memory implementation of a schema.
pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).map(|table| Arc::clone(table.value())))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return exec_err!("The table {name} already exists");
        }
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Schema;

    use crate::assert_batches_eq;
    use crate::catalog::schema::{MemorySchemaProvider, SchemaProvider};
    use crate::catalog::{CatalogProvider, MemoryCatalogProvider};
    use crate::datasource::empty::EmptyTable;
    use crate::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
    use crate::prelude::SessionContext;

    #[tokio::test]
    async fn test_mem_provider() {
        let provider = MemorySchemaProvider::new();
        let table_name = "test_table_exist";
        assert!(!provider.table_exist(table_name));
        assert!(provider.deregister_table(table_name).unwrap().is_none());
        let test_table = EmptyTable::new(Arc::new(Schema::empty()));
        // register table successfully
        assert!(provider
            .register_table(table_name.to_string(), Arc::new(test_table))
            .unwrap()
            .is_none());
        assert!(provider.table_exist(table_name));
        let other_table = EmptyTable::new(Arc::new(Schema::empty()));
        let result =
            provider.register_table(table_name.to_string(), Arc::new(other_table));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_register_listing_table() {
        let testdata = crate::test_util::parquet_test_data();
        let testdir = if testdata.starts_with('/') {
            format!("file://{testdata}")
        } else {
            format!("file:///{testdata}")
        };
        let filename = if testdir.ends_with('/') {
            format!("{}{}", testdir, "alltypes_plain.parquet")
        } else {
            format!("{}/{}", testdir, "alltypes_plain.parquet")
        };

        let table_path = ListingTableUrl::parse(filename).unwrap();

        let catalog = MemoryCatalogProvider::new();
        let schema = MemorySchemaProvider::new();

        let ctx = SessionContext::new();

        let config = ListingTableConfig::new(table_path)
            .infer(&ctx.state())
            .await
            .unwrap();
        let table = ListingTable::try_new(config).unwrap();

        schema
            .register_table("alltypes_plain".to_string(), Arc::new(table))
            .unwrap();

        catalog.register_schema("active", Arc::new(schema)).unwrap();
        ctx.register_catalog("cat", Arc::new(catalog));

        let df = ctx
            .sql("SELECT id, bool_col FROM cat.active.alltypes_plain")
            .await
            .unwrap();

        let actual = df.collect().await.unwrap();

        let expected = [
            "+----+----------+",
            "| id | bool_col |",
            "+----+----------+",
            "| 4  | true     |",
            "| 5  | false    |",
            "| 6  | true     |",
            "| 7  | false    |",
            "| 2  | true     |",
            "| 3  | false    |",
            "| 0  | true     |",
            "| 1  | false    |",
            "+----+----------+",
        ];
        assert_batches_eq!(expected, &actual);
    }
}
