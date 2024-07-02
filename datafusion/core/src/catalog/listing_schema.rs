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

//! listing_schema contains a SchemaProvider that scans ObjectStores for tables automatically

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::catalog::schema::SchemaProvider;
use crate::datasource::provider::TableProviderFactory;
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;

use datafusion_common::{Constraints, DFSchema, DataFusionError, TableReference};
use datafusion_expr::CreateExternalTable;

use async_trait::async_trait;
use futures::TryStreamExt;
use itertools::Itertools;
use object_store::ObjectStore;

/// A [`SchemaProvider`] that scans an [`ObjectStore`] to automatically discover tables
///
/// A subfolder relationship is assumed, i.e. given:
/// - authority = `s3://host.example.com:3000`
/// - path = `/data/tpch`
/// - factory = `DeltaTableFactory`
///
/// A table called "customer" will be registered for the folder:
/// `s3://host.example.com:3000/data/tpch/customer`
///
/// assuming it contains valid deltalake data, i.e:
/// - `s3://host.example.com:3000/data/tpch/customer/part-00000-xxxx.snappy.parquet`
/// - `s3://host.example.com:3000/data/tpch/customer/_delta_log/`
///
/// [`ObjectStore`]: object_store::ObjectStore
pub struct ListingSchemaProvider {
    authority: String,
    path: object_store::path::Path,
    factory: Arc<dyn TableProviderFactory>,
    store: Arc<dyn ObjectStore>,
    tables: Arc<Mutex<HashMap<String, Arc<dyn TableProvider>>>>,
    format: String,
}

impl ListingSchemaProvider {
    /// Create a new `ListingSchemaProvider`
    ///
    /// Arguments:
    /// `authority`: The scheme (i.e. s3://) + host (i.e. example.com:3000)
    /// `path`: The root path that contains subfolders which represent tables
    /// `factory`: The `TableProviderFactory` to use to instantiate tables for each subfolder
    /// `store`: The `ObjectStore` containing the table data
    /// `format`: The `FileFormat` of the tables
    /// `has_header`: Indicates whether the created external table has the has_header flag enabled
    pub fn new(
        authority: String,
        path: object_store::path::Path,
        factory: Arc<dyn TableProviderFactory>,
        store: Arc<dyn ObjectStore>,
        format: String,
    ) -> Self {
        Self {
            authority,
            path,
            factory,
            store,
            tables: Arc::new(Mutex::new(HashMap::new())),
            format,
        }
    }

    /// Reload table information from ObjectStore
    pub async fn refresh(&self, state: &SessionState) -> datafusion_common::Result<()> {
        let entries: Vec<_> = self.store.list(Some(&self.path)).try_collect().await?;
        let base = Path::new(self.path.as_ref());
        let mut tables = HashSet::new();
        for file in entries.iter() {
            // The listing will initially be a file. However if we've recursed up to match our base, we know our path is a directory.
            let mut is_dir = false;
            let mut parent = Path::new(file.location.as_ref());
            while let Some(p) = parent.parent() {
                if p == base {
                    tables.insert(TablePath {
                        is_dir,
                        path: parent,
                    });
                }
                parent = p;
                is_dir = true;
            }
        }
        for table in tables.iter() {
            let file_name = table
                .path
                .file_name()
                .ok_or_else(|| {
                    DataFusionError::Internal("Cannot parse file name!".to_string())
                })?
                .to_str()
                .ok_or_else(|| {
                    DataFusionError::Internal("Cannot parse file name!".to_string())
                })?;
            let table_name = file_name.split('.').collect_vec()[0];
            let table_path = table.to_string().ok_or_else(|| {
                DataFusionError::Internal("Cannot parse file name!".to_string())
            })?;

            if !self.table_exist(table_name) {
                let table_url = format!("{}/{}", self.authority, table_path);

                let name = TableReference::bare(table_name);
                let provider = self
                    .factory
                    .create(
                        state,
                        &CreateExternalTable {
                            schema: Arc::new(DFSchema::empty()),
                            name,
                            location: table_url,
                            file_type: self.format.clone(),
                            table_partition_cols: vec![],
                            if_not_exists: false,
                            definition: None,
                            order_exprs: vec![],
                            unbounded: false,
                            options: Default::default(),
                            constraints: Constraints::empty(),
                            column_defaults: Default::default(),
                        },
                    )
                    .await?;
                let _ =
                    self.register_table(table_name.to_string(), Arc::clone(&provider))?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SchemaProvider for ListingSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .keys()
            .map(|it| it.to_string())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self
            .tables
            .lock()
            .expect("Can't lock tables")
            .get(name)
            .cloned())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .insert(name, Arc::clone(&table));
        Ok(Some(table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.lock().expect("Can't lock tables").remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .lock()
            .expect("Can't lock tables")
            .contains_key(name)
    }
}

/// Stores metadata along with a table's path.
/// Primarily whether the path is a directory or not.
#[derive(Eq, PartialEq, Hash, Debug)]
struct TablePath<'a> {
    path: &'a Path,
    is_dir: bool,
}

impl TablePath<'_> {
    /// Format the path with a '/' appended if its a directory.
    /// Clients (eg. object_store listing) can and will use the presence of trailing slash as a heuristic
    fn to_string(&self) -> Option<String> {
        self.path.to_str().map(|path| {
            if self.is_dir {
                format!("{path}/")
            } else {
                path.to_string()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_path_ends_with_slash_when_is_dir() {
        let table_path = TablePath {
            path: Path::new("/file"),
            is_dir: true,
        };
        assert!(table_path.to_string().expect("table path").ends_with('/'));
    }

    #[test]
    fn dir_table_path_str_does_not_end_with_slash_when_not_is_dir() {
        let table_path = TablePath {
            path: Path::new("/file"),
            is_dir: false,
        };
        assert!(!table_path.to_string().expect("table_path").ends_with('/'));
    }
}
