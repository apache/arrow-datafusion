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

//! Interfaces and default implementations of catalogs and schemas.

pub mod information_schema;
pub mod listing_schema;
pub mod schema;

pub use datafusion_sql::{ResolvedTableReference, TableReference};

use crate::catalog::schema::SchemaProvider;
use dashmap::DashMap;
use datafusion_common::{exec_err, not_impl_err, Result};
use std::any::Any;
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

/// Represent a list of named [`CatalogProvider`]s.
///
/// Please see the documentation on `CatalogProvider` for details of
/// implementing a custom catalog.
pub trait CatalogProviderList: Sync + Send {
    /// Returns the catalog list as [`Any`]
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

/// See [`CatalogProviderList`]
#[deprecated(since = "35.0.0", note = "use [`CatalogProviderList`] instead")]
pub trait CatalogList: CatalogProviderList {}

/// Simple in-memory list of catalogs
pub struct MemoryCatalogProviderList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl MemoryCatalogProviderList {
    /// Instantiates a new `MemoryCatalogProviderList` with an empty collection of catalogs
    pub fn new() -> Self {
        Self {
            catalogs: DashMap::new(),
        }
    }
}

impl Default for MemoryCatalogProviderList {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogProviderList for MemoryCatalogProviderList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| Arc::clone(c.value()))
    }
}

/// Represents a catalog, comprising a number of named schemas.
///
/// # Catalog Overview
///
/// To plan and execute queries, DataFusion needs a "Catalog" that provides
/// metadata such as which schemas and tables exist, their columns and data
/// types, and how to access the data.
///
/// The Catalog API consists:
/// * [`CatalogProviderList`]: a collection of `CatalogProvider`s
/// * [`CatalogProvider`]: a collection of `SchemaProvider`s (sometimes called a "database" in other systems)
/// * [`SchemaProvider`]:  a collection of `TableProvider`s (often called a "schema" in other systems)
/// * [`TableProvider]`:  individual tables
///
/// # Implementing Catalogs
///
/// To implement a catalog, you implement at least one of the [`CatalogProviderList`],
/// [`CatalogProvider`] and [`SchemaProvider`] traits and register them
/// appropriately the [`SessionContext`].
///
/// [`SessionContext`]: crate::execution::context::SessionContext
///
/// DataFusion comes with a simple in-memory catalog implementation,
/// [`MemoryCatalogProvider`], that is used by default and has no persistence.
/// DataFusion does not include more complex Catalog implementations because
/// catalog management is a key design choice for most data systems, and thus
/// it is unlikely that any general-purpose catalog implementation will work
/// well across many use cases.
///
/// # Implementing "Remote" catalogs
///
/// Sometimes catalog information is stored remotely and requires a network call
/// to retrieve. For example, the [Delta Lake] table format stores table
/// metadata in files on S3 that must be first downloaded to discover what
/// schemas and tables exist.
///
/// [Delta Lake]: https://delta.io/
///
/// The [`CatalogProvider`] can support this use case, but it takes some care.
/// The planning APIs in DataFusion are not `async` and thus network IO can not
/// be performed "lazily" / "on demand" during query planning. The rationale for
/// this design is that using remote procedure calls for all catalog accesses
/// required for query planning would likely result in multiple network calls
/// per plan, resulting in very poor planning performance.
///
/// To implement [`CatalogProvider`] and [`SchemaProvider`] for remote catalogs,
/// you need to provide an in memory snapshot of the required metadata. Most
/// systems typically either already have this information cached locally or can
/// batch access to the remote catalog to retrieve multiple schemas and tables
/// in a single network call.
///
/// Note that [`SchemaProvider::table`] is an `async` function in order to
/// simplify implementing simple [`SchemaProvider`]s. For many table formats it
/// is easy to list all available tables but there is additional non trivial
/// access required to read table details (e.g. statistics).
///
/// The pattern that DataFusion itself uses to plan SQL queries is to walk over
/// the query to [find all table references],
/// performing required remote catalog in parallel, and then plans the query
/// using that snapshot.
///
/// [find all table references]: resolve_table_references
///
/// # Example Catalog Implementations
///
/// Here are some examples of how to implement custom catalogs:
///
/// * [`datafusion-cli`]: [`DynamicFileCatalogProvider`] catalog provider
/// that treats files and directories on a filesystem as tables.
///
/// * The [`catalog.rs`]:  a simple directory based catalog.
///
///  * [delta-rs]:  [`UnityCatalogProvider`] implementation that can
///  read from Delta Lake tables
///
/// [`datafusion-cli`]: https://datafusion.apache.org/user-guide/cli/index.html
/// [`DynamicFileCatalogProvider`]: https://github.com/apache/datafusion/blob/31b9b48b08592b7d293f46e75707aad7dadd7cbc/datafusion-cli/src/catalog.rs#L75
/// [`catalog.rs`]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/catalog.rs
/// [delta-rs]: https://github.com/delta-io/delta-rs
/// [`UnityCatalogProvider`]: https://github.com/delta-io/delta-rs/blob/951436ecec476ce65b5ed3b58b50fb0846ca7b91/crates/deltalake-core/src/data_catalog/unity/datafusion.rs#L111-L123
///
/// [`TableProvider]: crate::datasource::TableProvider

pub trait CatalogProvider: Sync + Send {
    /// Returns the catalog provider as [`Any`]
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
        not_impl_err!("Registering new schemas is not supported")
    }

    /// Removes a schema from this catalog. Implementations of this method should return
    /// errors if the schema exists but cannot be dropped. For example, in DataFusion's
    /// default in-memory catalog, [`MemoryCatalogProvider`], a non-empty schema
    /// will only be successfully dropped when `cascade` is true.
    /// This is equivalent to how DROP SCHEMA works in PostgreSQL.
    ///
    /// Implementations of this method should return None if schema with `name`
    /// does not exist.
    ///
    /// By default returns a "Not Implemented" error
    fn deregister_schema(
        &self,
        _name: &str,
        _cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        not_impl_err!("Deregistering new schemas is not supported")
    }
}

/// Simple in-memory implementation of a catalog.
pub struct MemoryCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl MemoryCatalogProvider {
    /// Instantiates a new MemoryCatalogProvider with an empty collection of schemas.
    pub fn new() -> Self {
        Self {
            schemas: DashMap::new(),
        }
    }
}

impl Default for MemoryCatalogProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|s| Arc::clone(s.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if let Some(schema) = self.schema(name) {
            let table_names = schema.table_names();
            match (table_names.is_empty(), cascade) {
                (true, _) | (false, true) => {
                    let (_, removed) = self.schemas.remove(name).unwrap();
                    Ok(Some(removed))
                }
                (false, false) => exec_err!(
                    "Cannot drop schema {} because other tables depend on it: {}",
                    name,
                    itertools::join(table_names.iter(), ", ")
                ),
            }
        } else {
            Ok(None)
        }
    }
}

/// Collects all tables and views referenced in the SQL statement. CTEs are collected separately.
/// This can be used to determine which tables need to be in the catalog for a query to be planned.
///
/// # Returns
///
/// A `(table_refs, ctes)` tuple, the first element contains table and view references and the second
/// element contains any CTE aliases that were defined and possibly referenced.
///
/// ## Example
///
/// ```
/// # use datafusion_sql::parser::DFParser;
/// # use datafusion::catalog::resolve_table_references;
/// let query = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
/// let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
/// assert_eq!(table_refs.len(), 2);
/// assert_eq!(table_refs[0].to_string(), "bar");
/// assert_eq!(table_refs[1].to_string(), "foo");
/// assert_eq!(ctes.len(), 0);
/// ```
///
/// ## Example with CTEs  
///  
/// ```  
/// # use datafusion_sql::parser::DFParser;  
/// # use datafusion::catalog::resolve_table_references;  
/// let query = "with my_cte as (values (1), (2)) SELECT * from my_cte;";  
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();  
/// let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();  
/// assert_eq!(table_refs.len(), 0);
/// assert_eq!(ctes.len(), 1);  
/// assert_eq!(ctes[0].to_string(), "my_cte");  
/// ```
pub fn resolve_table_references(
    statement: &datafusion_sql::parser::Statement,
    enable_ident_normalization: bool,
) -> datafusion_common::Result<(Vec<TableReference>, Vec<TableReference>)> {
    use crate::sql::planner::object_name_to_table_reference;
    use datafusion_sql::parser::{
        CopyToSource, CopyToStatement, Statement as DFStatement,
    };
    use information_schema::INFORMATION_SCHEMA;
    use information_schema::INFORMATION_SCHEMA_TABLES;
    use sqlparser::ast::*;

    struct RelationVisitor {
        relations: BTreeSet<ObjectName>,
        all_ctes: BTreeSet<ObjectName>,
        ctes_in_scope: Vec<ObjectName>,
    }

    impl RelationVisitor {
        /// Record the reference to `relation`, if it's not a CTE reference.
        fn insert_relation(&mut self, relation: &ObjectName) {
            if !self.relations.contains(relation)
                && !self.ctes_in_scope.contains(relation)
            {
                self.relations.insert(relation.clone());
            }
        }
    }

    impl Visitor for RelationVisitor {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
            self.insert_relation(relation);
            ControlFlow::Continue(())
        }

        fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &q.with {
                for cte in &with.cte_tables {
                    // The non-recursive CTE name is not in scope when evaluating the CTE itself, so this is valid:
                    // `WITH t AS (SELECT * FROM t) SELECT * FROM t`
                    // Where the first `t` refers to a predefined table. So we are careful here
                    // to visit the CTE first, before putting it in scope.
                    if !with.recursive {
                        // This is a bit hackish as the CTE will be visited again as part of visiting `q`,
                        // but thankfully `insert_relation` is idempotent.
                        cte.visit(self);
                    }
                    self.ctes_in_scope
                        .push(ObjectName(vec![cte.alias.name.clone()]));
                }
            }
            ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
            if let Some(with) = &q.with {
                for _ in &with.cte_tables {
                    // Unwrap: We just pushed these in `pre_visit_query`
                    self.all_ctes.insert(self.ctes_in_scope.pop().unwrap());
                }
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            if let Statement::ShowCreate {
                obj_type: ShowCreateObject::Table | ShowCreateObject::View,
                obj_name,
            } = statement
            {
                self.insert_relation(obj_name)
            }

            // SHOW statements will later be rewritten into a SELECT from the information_schema
            let requires_information_schema = matches!(
                statement,
                Statement::ShowFunctions { .. }
                    | Statement::ShowVariable { .. }
                    | Statement::ShowStatus { .. }
                    | Statement::ShowVariables { .. }
                    | Statement::ShowCreate { .. }
                    | Statement::ShowColumns { .. }
                    | Statement::ShowTables { .. }
                    | Statement::ShowCollation { .. }
            );
            if requires_information_schema {
                for s in INFORMATION_SCHEMA_TABLES {
                    self.relations.insert(ObjectName(vec![
                        Ident::new(INFORMATION_SCHEMA),
                        Ident::new(*s),
                    ]));
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
        all_ctes: BTreeSet::new(),
        ctes_in_scope: vec![],
    };

    fn visit_statement(statement: &DFStatement, visitor: &mut RelationVisitor) {
        match statement {
            DFStatement::Statement(s) => {
                let _ = s.as_ref().visit(visitor);
            }
            DFStatement::CreateExternalTable(table) => {
                visitor
                    .relations
                    .insert(ObjectName(vec![Ident::from(table.name.as_str())]));
            }
            DFStatement::CopyTo(CopyToStatement { source, .. }) => match source {
                CopyToSource::Relation(table_name) => {
                    visitor.insert_relation(table_name);
                }
                CopyToSource::Query(query) => {
                    query.visit(visitor);
                }
            },
            DFStatement::Explain(explain) => visit_statement(&explain.statement, visitor),
        }
    }

    visit_statement(statement, &mut visitor);

    let table_refs = visitor
        .relations
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion_common::Result<_>>()?;
    let ctes = visitor
        .all_ctes
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion_common::Result<_>>()?;
    Ok((table_refs, ctes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::MemorySchemaProvider;
    use crate::datasource::empty::EmptyTable;
    use crate::datasource::TableProvider;
    use arrow::datatypes::Schema;

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

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        let catalog = Arc::new(TestProvider {});

        match catalog.register_schema("foo", schema) {
            Ok(_) => panic!("unexpected OK"),
            Err(e) => assert_eq!(e.strip_backtrace(), "This feature is not implemented: Registering new schemas is not supported"),
        };
    }

    #[test]
    fn memory_catalog_dereg_nonempty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        let test_table = Arc::new(EmptyTable::new(Arc::new(Schema::empty())))
            as Arc<dyn TableProvider>;
        schema.register_table("t".into(), test_table).unwrap();

        cat.register_schema("foo", schema.clone()).unwrap();

        assert!(
            cat.deregister_schema("foo", false).is_err(),
            "dropping empty schema without cascade should error"
        );
        assert!(cat.deregister_schema("foo", true).unwrap().is_some());
    }

    #[test]
    fn memory_catalog_dereg_empty_schema() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;

        let schema = Arc::new(MemorySchemaProvider::new()) as Arc<dyn SchemaProvider>;
        cat.register_schema("foo", schema).unwrap();

        assert!(cat.deregister_schema("foo", false).unwrap().is_some());
    }

    #[test]
    fn memory_catalog_dereg_missing() {
        let cat = Arc::new(MemoryCatalogProvider::new()) as Arc<dyn CatalogProvider>;
        assert!(cat.deregister_schema("foo", false).unwrap().is_none());
    }

    #[test]
    fn resolve_table_references_shadowed_cte() {
        use datafusion_sql::parser::DFParser;

        // An interesting edge case where the `t` name is used both as an ordinary table reference
        // and as a CTE reference.
        let query = "WITH t AS (SELECT * FROM t) SELECT * FROM t";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(table_refs[0].to_string(), "t");

        // UNION is a special case where the CTE is not in scope for the second branch.
        let query = "(with t as (select 1) select * from t) union (select * from t)";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(table_refs[0].to_string(), "t");

        // Nested CTEs are also handled.
        // Here the first `u` is a CTE, but the second `u` is a table reference.
        // While `t` is always a CTE.
        let query = "(with t as (with u as (select 1) select * from u) select * from u cross join t)";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 2);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(ctes[1].to_string(), "u");
        assert_eq!(table_refs[0].to_string(), "u");
    }

    #[test]
    fn resolve_table_references_recursive_cte() {
        use datafusion_sql::parser::DFParser;

        let query = "
            WITH RECURSIVE nodes AS ( 
                SELECT 1 as id
                UNION ALL 
                SELECT id + 1 as id 
                FROM nodes
                WHERE id < 10
            )
            SELECT * FROM nodes
        ";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 0);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "nodes");
    }
}
