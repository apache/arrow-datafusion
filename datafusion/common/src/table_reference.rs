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

use crate::utils::{parse_identifiers_normalized, quote_identifier};
use std::{borrow::Cow, marker::PhantomData, sync::Arc};

/// A resolved path to a table of the form "catalog.schema.table"
#[derive(Debug, Clone)]
pub struct ResolvedTableReference<'a> {
    /// The catalog (aka database) containing the table
    pub catalog: Arc<String>,
    /// The schema containing the table
    pub schema: Arc<String>,
    /// The table name
    pub table: Arc<String>,
    phantom: PhantomData<&'a bool>,
}

impl<'a> std::fmt::Display for ResolvedTableReference<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// [`TableReference`]s represent a multi part identifier (path) to a
/// table that may require further resolution.
///
/// # Creating [`TableReference`]
///
/// When converting strings to [`TableReference`]s, the string is
/// parsed as though it were a SQL identifier, normalizing (convert to
/// lowercase) any unquoted identifiers.
///
/// See [`TableReference::bare`] to create references without applying
/// normalization semantics
///
/// # Examples
/// ```
/// # use datafusion_common::TableReference;
/// // Get a table reference to 'mytable'
/// let table_reference = TableReference::from("mytable");
/// assert_eq!(table_reference, TableReference::bare("mytable"));
///
/// // Get a table reference to 'mytable' (note the capitalization)
/// let table_reference = TableReference::from("MyTable");
/// assert_eq!(table_reference, TableReference::bare("mytable"));
///
/// // Get a table reference to 'MyTable' (note the capitalization) using double quotes
/// // (programatically it is better to use `TableReference::bare` for this)
/// let table_reference = TableReference::from(r#""MyTable""#);
/// assert_eq!(table_reference, TableReference::bare("MyTable"));
///
/// // Get a table reference to 'myschema.mytable' (note the capitalization)
/// let table_reference = TableReference::from("MySchema.MyTable");
/// assert_eq!(table_reference, TableReference::partial("myschema", "mytable"));
///```
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableReference<'a> {
    X {
        phantom: PhantomData<&'a str>,
    },
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: Arc<String>,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: Arc<String>,
        /// The table name
        table: Arc<String>,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: Arc<String>,
        /// The schema containing the table
        schema: Arc<String>,
        /// The table name
        table: Arc<String>,
    },
}

/// This is a [`TableReference`] that has 'static lifetime (aka it
/// owns the underlying string)
///
/// To  convert a [`TableReference`] to an [`OwnedTableReference`], use
///
/// ```
/// # use datafusion_common::{OwnedTableReference, TableReference};
/// let table_reference = TableReference::from("mytable");
/// let owned_reference = table_reference.to_owned_reference();
/// ```
pub type OwnedTableReference = TableReference<'static>;

impl std::fmt::Display for TableReference<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableReference::Bare { table } => write!(f, "{table}"),
            TableReference::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
            _ => unreachable!()
        }
    }
}

impl<'a> TableReference<'a> {
    /// Convenience method for creating a typed none `None`
    pub fn none() -> Option<TableReference<'a>> {
        None
    }

    /// Convenience method for creating a [`TableReference::Bare`]
    ///
    /// As described on [`TableReference`] this does *NO* parsing at
    /// all, so "Foo.Bar" stays as a reference to the table named
    /// "Foo.Bar" (rather than "foo"."bar")
    pub fn bare(table: impl Into<String>) -> TableReference<'a> {
        TableReference::Bare {
            table: Arc::new(table.into())
        }
    }

    /// Convenience method for creating a [`TableReference::Partial`].
    ///
    /// As described on [`TableReference`] this does *NO* parsing at all.
    pub fn partial(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> TableReference<'a> {
        TableReference::Partial {
            schema: Arc::new(schema.into()),
            table: Arc::new(table.into())
        }
    }

    /// Convenience method for creating a [`TableReference::Full`]
    ///
    /// As described on [`TableReference`] this does *NO* parsing at all.
    pub fn full(
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> TableReference<'a> {
        TableReference::Full {
            catalog: Arc::new(catalog.into()),
            schema: Arc::new(schema.into()),
            table: Arc::new(table.into())
        }
    }

    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. }
            | Self::Partial { table, .. }
            | Self::Bare { table } => table,
            _ => unreachable!()
        }
    }

    /// Retrieve the schema name if in the `Partial` or `Full` qualification
    pub fn schema(&self) -> Option<&str> {
        match self {
            Self::Full { schema, .. } | Self::Partial { schema, .. } => Some(schema),
            _ => None,
        }
    }

    /// Retrieve the catalog name if in the `Full` qualification
    pub fn catalog(&self) -> Option<&str> {
        match self {
            Self::Full { catalog, .. } => Some(catalog),
            _ => None,
        }
    }

    /// Compare with another [`TableReference`] as if both are resolved.
    /// This allows comparing across variants, where if a field is not present
    /// in both variants being compared then it is ignored in the comparison.
    ///
    /// e.g. this allows a [`TableReference::Bare`] to be considered equal to a
    /// fully qualified [`TableReference::Full`] if the table names match.
    pub fn resolved_eq(&self, other: &Self) -> bool {
        match self {
            TableReference::Bare { table } => **table == other.table(),
            TableReference::Partial { schema, table } => {
                **table == other.table() && other.schema().map_or(true, |s| s == **schema)
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => {
                **table == other.table()
                    && other.schema().map_or(true, |s| s == **schema)
                    && other.catalog().map_or(true, |c| c == **catalog)
            }
            TableReference::X { phantom } => todo!(),
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully resolved
    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
                phantom: PhantomData
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: Arc::new(default_catalog.into()),
                schema,
                table,
                phantom: PhantomData
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: Arc::new(default_catalog.into()),
                schema: Arc::new(default_schema.into()),
                table,
                phantom: PhantomData
            },
            _ => unreachable!()
        }
    }

    /// Converts directly into an [`OwnedTableReference`] by cloning
    /// the underlying data.
    pub fn to_owned_reference(&self) -> OwnedTableReference {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => OwnedTableReference::Full {
                catalog: catalog.to_string().into(),
                schema: schema.to_string().into(),
                table: table.to_string().into(),
            },
            Self::Partial { schema, table } => OwnedTableReference::Partial {
                schema: schema.to_string().into(),
                table: table.to_string().into(),
            },
            Self::Bare { table } => OwnedTableReference::Bare {
                table: table.to_string().into(),
            },
            _ => unreachable!()
        }
    }

    /// Forms a string where the identifiers are quoted
    ///
    /// # Example
    /// ```
    /// # use datafusion_common::TableReference;
    /// let table_reference = TableReference::partial("myschema", "mytable");
    /// assert_eq!(table_reference.to_quoted_string(), "myschema.mytable");
    ///
    /// let table_reference = TableReference::partial("MySchema", "MyTable");
    /// assert_eq!(table_reference.to_quoted_string(), r#""MySchema"."MyTable""#);
    /// ```
    pub fn to_quoted_string(&self) -> String {
        match self {
            TableReference::Bare { table } => quote_identifier(table).to_string(),
            TableReference::Partial { schema, table } => {
                format!("{}.{}", quote_identifier(schema), quote_identifier(table))
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => format!(
                "{}.{}.{}",
                quote_identifier(catalog),
                quote_identifier(schema),
                quote_identifier(table)
            ),
            _ => unreachable!()
        }
    }

    /// Forms a [`TableReference`] by parsing `s` as a multipart SQL
    /// identifier. See docs on [`TableReference`] for more details.
    pub fn parse_str(s: &'a str) -> Self {
        let mut parts = parse_identifiers_normalized(s, false);

        match parts.len() {
            1 => Self::Bare {
                table: parts.remove(0).into(),
            },
            2 => Self::Partial {
                schema: parts.remove(0).into(),
                table: parts.remove(0).into(),
            },
            3 => Self::Full {
                catalog: parts.remove(0).into(),
                schema: parts.remove(0).into(),
                table: parts.remove(0).into(),
            },
            _ => Self::Bare { table: Arc::new(s.into()) },
        }
    }

    /// Decompose a [`TableReference`] to separate parts. The result vector contains
    /// at most three elements in the following sequence:
    /// ```no_rust
    /// [<catalog>, <schema>, table]
    /// ```
    pub fn to_vec(&self) -> Vec<String> {
        match self {
            TableReference::Bare { table } => vec![table.to_string()],
            TableReference::Partial { schema, table } => {
                vec![schema.to_string(), table.to_string()]
            }
            TableReference::Full {
                catalog,
                schema,
                table,
            } => vec![catalog.to_string(), schema.to_string(), table.to_string()],
            _  => unreachable!()
        }
    }
}

/// Parse a `String` into a OwnedTableReference as a multipart SQL identifier.
impl From<String> for OwnedTableReference {
    fn from(s: String) -> Self {
        TableReference::parse_str(&s).to_owned_reference()
    }
}

impl<'a> From<&'a OwnedTableReference> for TableReference<'a> {
    fn from(value: &'a OwnedTableReference) -> Self {
        value.clone()
    }
}

/// Parse a string into a TableReference, normalizing where appropriate
///
/// See full details on [`TableReference::parse_str`]
impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::parse_str(s)
    }
}

impl<'a> From<&'a String> for TableReference<'a> {
    fn from(s: &'a String) -> Self {
        Self::parse_str(s)
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableReference<'a> {
    fn from(resolved: ResolvedTableReference<'a>) -> Self {
        Self::Full {
            catalog: resolved.catalog,
            schema: resolved.schema,
            table: resolved.table,
        }
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_table_reference_from_str_normalizes() {
//         let expected = TableReference::Full {
//             catalog: Cow::Owned("catalog".to_string()),
//             schema: Cow::Owned("FOO\".bar".to_string()),
//             table: Cow::Owned("table".to_string()),
//         };
//         let actual = TableReference::from("catalog.\"FOO\"\".bar\".TABLE");
//         assert_eq!(expected, actual);

//         let expected = TableReference::Partial {
//             schema: Cow::Owned("FOO\".bar".to_string()),
//             table: Cow::Owned("table".to_string()),
//         };
//         let actual = TableReference::from("\"FOO\"\".bar\".TABLE");
//         assert_eq!(expected, actual);

//         let expected = TableReference::Bare {
//             table: Cow::Owned("table".to_string()),
//         };
//         let actual = TableReference::from("TABLE");
//         assert_eq!(expected, actual);

//         // if fail to parse, take entire input string as identifier
//         let expected = TableReference::Bare {
//             table: Cow::Owned("TABLE()".to_string()),
//         };
//         let actual = TableReference::from("TABLE()");
//         assert_eq!(expected, actual);
//     }

//     #[test]
//     fn test_table_reference_to_vector() {
//         let table_reference = TableReference::parse_str("table");
//         assert_eq!(vec!["table".to_string()], table_reference.to_vec());

//         let table_reference = TableReference::parse_str("schema.table");
//         assert_eq!(
//             vec!["schema".to_string(), "table".to_string()],
//             table_reference.to_vec()
//         );

//         let table_reference = TableReference::parse_str("catalog.schema.table");
//         assert_eq!(
//             vec![
//                 "catalog".to_string(),
//                 "schema".to_string(),
//                 "table".to_string()
//             ],
//             table_reference.to_vec()
//         );
//     }
// }
