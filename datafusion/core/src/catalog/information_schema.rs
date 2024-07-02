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

//! Implements the SQL [Information Schema] for DataFusion.
//!
//! [Information Schema]: https://en.wikipedia.org/wiki/Information_schema

use async_trait::async_trait;
use datafusion_common::DataFusionError;
use std::{any::Any, sync::Arc};

use arrow::{
    array::{StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use crate::datasource::streaming::StreamingTable;
use crate::datasource::TableProvider;
use crate::execution::context::TaskContext;
use crate::logical_expr::TableType;
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::SendableRecordBatchStream;
use crate::{
    config::{ConfigEntry, ConfigOptions},
    physical_plan::streaming::PartitionStream,
};

use super::{schema::SchemaProvider, CatalogProviderList};

pub(crate) const INFORMATION_SCHEMA: &str = "information_schema";
pub(crate) const TABLES: &str = "tables";
pub(crate) const VIEWS: &str = "views";
pub(crate) const COLUMNS: &str = "columns";
pub(crate) const DF_SETTINGS: &str = "df_settings";
pub(crate) const SCHEMATA: &str = "schemata";

/// All information schema tables
pub const INFORMATION_SCHEMA_TABLES: &[&str] =
    &[TABLES, VIEWS, COLUMNS, DF_SETTINGS, SCHEMATA];

/// Implements the `information_schema` virtual schema and tables
///
/// The underlying tables in the `information_schema` are created on
/// demand. This means that if more tables are added to the underlying
/// providers, they will appear the next time the `information_schema`
/// table is queried.
pub struct InformationSchemaProvider {
    config: InformationSchemaConfig,
}

impl InformationSchemaProvider {
    /// Creates a new [`InformationSchemaProvider`] for the provided `catalog_list`
    pub fn new(catalog_list: Arc<dyn CatalogProviderList>) -> Self {
        Self {
            config: InformationSchemaConfig { catalog_list },
        }
    }
}

#[derive(Clone)]
struct InformationSchemaConfig {
    catalog_list: Arc<dyn CatalogProviderList>,
}

impl InformationSchemaConfig {
    /// Construct the `information_schema.tables` virtual table
    async fn make_tables(
        &self,
        builder: &mut InformationSchemaTablesBuilder,
    ) -> Result<(), DataFusionError> {
        // create a mem table with the names of tables

        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    // schema name may not exist in the catalog, so we need to check
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            if let Some(table) = schema.table(&table_name).await? {
                                builder.add_table(
                                    &catalog_name,
                                    &schema_name,
                                    &table_name,
                                    table.table_type(),
                                );
                            }
                        }
                    }
                }
            }

            // Add a final list for the information schema tables themselves
            for table_name in INFORMATION_SCHEMA_TABLES {
                builder.add_table(
                    &catalog_name,
                    INFORMATION_SCHEMA,
                    table_name,
                    TableType::View,
                );
            }
        }

        Ok(())
    }

    async fn make_schemata(&self, builder: &mut InformationSchemataBuilder) {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    if let Some(schema) = catalog.schema(&schema_name) {
                        let schema_owner = schema.owner_name();
                        builder.add_schemata(&catalog_name, &schema_name, schema_owner);
                    }
                }
            }
        }
    }

    async fn make_views(
        &self,
        builder: &mut InformationSchemaViewBuilder,
    ) -> Result<(), DataFusionError> {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    // schema name may not exist in the catalog, so we need to check
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            if let Some(table) = schema.table(&table_name).await? {
                                builder.add_view(
                                    &catalog_name,
                                    &schema_name,
                                    &table_name,
                                    table.get_table_definition(),
                                )
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Construct the `information_schema.columns` virtual table
    async fn make_columns(
        &self,
        builder: &mut InformationSchemaColumnsBuilder,
    ) -> Result<(), DataFusionError> {
        for catalog_name in self.catalog_list.catalog_names() {
            let catalog = self.catalog_list.catalog(&catalog_name).unwrap();

            for schema_name in catalog.schema_names() {
                if schema_name != INFORMATION_SCHEMA {
                    // schema name may not exist in the catalog, so we need to check
                    if let Some(schema) = catalog.schema(&schema_name) {
                        for table_name in schema.table_names() {
                            if let Some(table) = schema.table(&table_name).await? {
                                for (field_position, field) in
                                    table.schema().fields().iter().enumerate()
                                {
                                    builder.add_column(
                                        &catalog_name,
                                        &schema_name,
                                        &table_name,
                                        field_position,
                                        &field.as_ref().clone().into(),
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Construct the `information_schema.df_settings` virtual table
    fn make_df_settings(
        &self,
        config_options: &ConfigOptions,
        builder: &mut InformationSchemaDfSettingsBuilder,
    ) {
        for entry in config_options.entries() {
            builder.add_setting(entry);
        }
    }
}

#[async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        INFORMATION_SCHEMA_TABLES
            .iter()
            .map(|t| t.to_string())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let config = self.config.clone();
        let table: Arc<dyn PartitionStream> = match name.to_ascii_lowercase().as_str() {
            TABLES => Arc::new(InformationSchemaTables::new(config)),
            COLUMNS => Arc::new(InformationSchemaColumns::new(config)),
            VIEWS => Arc::new(InformationSchemaViews::new(config)),
            DF_SETTINGS => Arc::new(InformationSchemaDfSettings::new(config)),
            SCHEMATA => Arc::new(InformationSchemata::new(config)),
            _ => return Ok(None),
        };

        Ok(Some(Arc::new(
            StreamingTable::try_new(table.schema().clone(), vec![table]).unwrap(),
        )))
    }

    fn table_exist(&self, name: &str) -> bool {
        INFORMATION_SCHEMA_TABLES.contains(&name.to_ascii_lowercase().as_str())
    }
}

struct InformationSchemaTables {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaTables {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaTablesBuilder {
        InformationSchemaTablesBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            table_types: StringBuilder::new(),
            schema: self.schema.clone(),
        }
    }
}

impl PartitionStream for InformationSchemaTables {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_tables(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

/// Builds the `information_schema.TABLE` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaTablesBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    fn add_table(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: TableType,
    ) {
        // Note: append_value is actually infallable.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.table_types.append_value(match table_type {
            TableType::Base => "BASE TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        });
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.table_types.finish()),
            ],
        )
        .unwrap()
    }
}

struct InformationSchemaViews {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaViews {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("definition", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaViewBuilder {
        InformationSchemaViewBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            definitions: StringBuilder::new(),
            schema: self.schema.clone(),
        }
    }
}

impl PartitionStream for InformationSchemaViews {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_views(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

/// Builds the `information_schema.VIEWS` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaViewBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    definitions: StringBuilder,
}

impl InformationSchemaViewBuilder {
    fn add_view(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        definition: Option<impl AsRef<str>>,
    ) {
        // Note: append_value is actually infallable.
        self.catalog_names.append_value(catalog_name.as_ref());
        self.schema_names.append_value(schema_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.definitions.append_option(definition.as_ref());
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.definitions.finish()),
            ],
        )
        .unwrap()
    }
}

struct InformationSchemaColumns {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaColumns {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::UInt64, false),
            Field::new("column_default", DataType::Utf8, true),
            Field::new("is_nullable", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("character_maximum_length", DataType::UInt64, true),
            Field::new("character_octet_length", DataType::UInt64, true),
            Field::new("numeric_precision", DataType::UInt64, true),
            Field::new("numeric_precision_radix", DataType::UInt64, true),
            Field::new("numeric_scale", DataType::UInt64, true),
            Field::new("datetime_precision", DataType::UInt64, true),
            Field::new("interval_type", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaColumnsBuilder {
        // StringBuilder requires providing an initial capacity, so
        // pick 10 here arbitrarily as this is not performance
        // critical code and the number of tables is unavailable here.
        let default_capacity = 10;

        InformationSchemaColumnsBuilder {
            catalog_names: StringBuilder::new(),
            schema_names: StringBuilder::new(),
            table_names: StringBuilder::new(),
            column_names: StringBuilder::new(),
            ordinal_positions: UInt64Builder::with_capacity(default_capacity),
            column_defaults: StringBuilder::new(),
            is_nullables: StringBuilder::new(),
            data_types: StringBuilder::new(),
            character_maximum_lengths: UInt64Builder::with_capacity(default_capacity),
            character_octet_lengths: UInt64Builder::with_capacity(default_capacity),
            numeric_precisions: UInt64Builder::with_capacity(default_capacity),
            numeric_precision_radixes: UInt64Builder::with_capacity(default_capacity),
            numeric_scales: UInt64Builder::with_capacity(default_capacity),
            datetime_precisions: UInt64Builder::with_capacity(default_capacity),
            interval_types: StringBuilder::new(),
            schema: self.schema.clone(),
        }
    }
}

impl PartitionStream for InformationSchemaColumns {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_columns(&mut builder).await?;
                Ok(builder.finish())
            }),
        ))
    }
}

/// Builds the `information_schema.COLUMNS` table row by row
///
/// Columns are based on <https://www.postgresql.org/docs/current/infoschema-columns.html>
struct InformationSchemaColumnsBuilder {
    schema: SchemaRef,
    catalog_names: StringBuilder,
    schema_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    ordinal_positions: UInt64Builder,
    column_defaults: StringBuilder,
    is_nullables: StringBuilder,
    data_types: StringBuilder,
    character_maximum_lengths: UInt64Builder,
    character_octet_lengths: UInt64Builder,
    numeric_precisions: UInt64Builder,
    numeric_precision_radixes: UInt64Builder,
    numeric_scales: UInt64Builder,
    datetime_precisions: UInt64Builder,
    interval_types: StringBuilder,
}

impl InformationSchemaColumnsBuilder {
    fn add_column(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        field_position: usize,
        field: &Field,
    ) {
        use DataType::*;

        // Note: append_value is actually infallable.
        self.catalog_names.append_value(catalog_name);
        self.schema_names.append_value(schema_name);
        self.table_names.append_value(table_name);

        self.column_names.append_value(field.name());

        self.ordinal_positions.append_value(field_position as u64);

        // DataFusion does not support column default values, so null
        self.column_defaults.append_null();

        // "YES if the column is possibly nullable, NO if it is known not nullable. "
        let nullable_str = if field.is_nullable() { "YES" } else { "NO" };
        self.is_nullables.append_value(nullable_str);

        // "System supplied type" --> Use debug format of the datatype
        self.data_types
            .append_value(format!("{:?}", field.data_type()));

        // "If data_type identifies a character or bit string type, the
        // declared maximum length; null for all other data types or
        // if no maximum length was declared."
        //
        // Arrow has no equivalent of VARCHAR(20), so we leave this as Null
        let max_chars = None;
        self.character_maximum_lengths.append_option(max_chars);

        // "Maximum length, in bytes, for binary data, character data,
        // or text and image data."
        let char_len: Option<u64> = match field.data_type() {
            Utf8 | Binary => Some(i32::MAX as u64),
            LargeBinary | LargeUtf8 => Some(i64::MAX as u64),
            _ => None,
        };
        self.character_octet_lengths.append_option(char_len);

        // numeric_precision: "If data_type identifies a numeric type, this column
        // contains the (declared or implicit) precision of the type
        // for this column. The precision indicates the number of
        // significant digits. It can be expressed in decimal (base
        // 10) or binary (base 2) terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null."
        //
        // numeric_radix: If data_type identifies a numeric type, this
        // column indicates in which base the values in the columns
        // numeric_precision and numeric_scale are expressed. The
        // value is either 2 or 10. For all other data types, this
        // column is null.
        //
        // numeric_scale: If data_type identifies an exact numeric
        // type, this column contains the (declared or implicit) scale
        // of the type for this column. The scale indicates the number
        // of significant digits to the right of the decimal point. It
        // can be expressed in decimal (base 10) or binary (base 2)
        // terms, as specified in the column
        // numeric_precision_radix. For all other data types, this
        // column is null.
        let (numeric_precision, numeric_radix, numeric_scale) = match field.data_type() {
            Int8 | UInt8 => (Some(8), Some(2), None),
            Int16 | UInt16 => (Some(16), Some(2), None),
            Int32 | UInt32 => (Some(32), Some(2), None),
            // From max value of 65504 as explained on
            // https://en.wikipedia.org/wiki/Half-precision_floating-point_format#Exponent_encoding
            Float16 => (Some(15), Some(2), None),
            // Numbers from postgres `real` type
            Float32 => (Some(24), Some(2), None),
            // Numbers from postgres `double` type
            Float64 => (Some(24), Some(2), None),
            Decimal128(precision, scale) => {
                (Some(*precision as u64), Some(10), Some(*scale as u64))
            }
            _ => (None, None, None),
        };

        self.numeric_precisions.append_option(numeric_precision);
        self.numeric_precision_radixes.append_option(numeric_radix);
        self.numeric_scales.append_option(numeric_scale);

        self.datetime_precisions.append_option(None);
        self.interval_types.append_null();
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.catalog_names.finish()),
                Arc::new(self.schema_names.finish()),
                Arc::new(self.table_names.finish()),
                Arc::new(self.column_names.finish()),
                Arc::new(self.ordinal_positions.finish()),
                Arc::new(self.column_defaults.finish()),
                Arc::new(self.is_nullables.finish()),
                Arc::new(self.data_types.finish()),
                Arc::new(self.character_maximum_lengths.finish()),
                Arc::new(self.character_octet_lengths.finish()),
                Arc::new(self.numeric_precisions.finish()),
                Arc::new(self.numeric_precision_radixes.finish()),
                Arc::new(self.numeric_scales.finish()),
                Arc::new(self.datetime_precisions.finish()),
                Arc::new(self.interval_types.finish()),
            ],
        )
        .unwrap()
    }
}

struct InformationSchemata {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemata {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("schema_owner", DataType::Utf8, true),
            Field::new("default_character_set_catalog", DataType::Utf8, true),
            Field::new("default_character_set_schema", DataType::Utf8, true),
            Field::new("default_character_set_name", DataType::Utf8, true),
            Field::new("sql_path", DataType::Utf8, true),
        ]));
        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemataBuilder {
        InformationSchemataBuilder {
            schema: self.schema.clone(),
            catalog_name: StringBuilder::new(),
            schema_name: StringBuilder::new(),
            schema_owner: StringBuilder::new(),
            default_character_set_catalog: StringBuilder::new(),
            default_character_set_schema: StringBuilder::new(),
            default_character_set_name: StringBuilder::new(),
            sql_path: StringBuilder::new(),
        }
    }
}

struct InformationSchemataBuilder {
    schema: SchemaRef,
    catalog_name: StringBuilder,
    schema_name: StringBuilder,
    schema_owner: StringBuilder,
    default_character_set_catalog: StringBuilder,
    default_character_set_schema: StringBuilder,
    default_character_set_name: StringBuilder,
    sql_path: StringBuilder,
}

impl InformationSchemataBuilder {
    fn add_schemata(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        schema_owner: Option<&str>,
    ) {
        self.catalog_name.append_value(catalog_name);
        self.schema_name.append_value(schema_name);
        match schema_owner {
            Some(owner) => self.schema_owner.append_value(owner),
            None => self.schema_owner.append_null(),
        }
        // refer to https://www.postgresql.org/docs/current/infoschema-schemata.html,
        // these rows apply to a feature that is not implemented in DataFusion
        self.default_character_set_catalog.append_null();
        self.default_character_set_schema.append_null();
        self.default_character_set_name.append_null();
        self.sql_path.append_null();
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.catalog_name.finish()),
                Arc::new(self.schema_name.finish()),
                Arc::new(self.schema_owner.finish()),
                Arc::new(self.default_character_set_catalog.finish()),
                Arc::new(self.default_character_set_schema.finish()),
                Arc::new(self.default_character_set_name.finish()),
                Arc::new(self.sql_path.finish()),
            ],
        )
        .unwrap()
    }
}

impl PartitionStream for InformationSchemata {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut builder = self.builder();
        let config = self.config.clone();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                config.make_schemata(&mut builder).await;
                Ok(builder.finish())
            }),
        ))
    }
}

struct InformationSchemaDfSettings {
    schema: SchemaRef,
    config: InformationSchemaConfig,
}

impl InformationSchemaDfSettings {
    fn new(config: InformationSchemaConfig) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
        ]));

        Self { schema, config }
    }

    fn builder(&self) -> InformationSchemaDfSettingsBuilder {
        InformationSchemaDfSettingsBuilder {
            names: StringBuilder::new(),
            values: StringBuilder::new(),
            descriptions: StringBuilder::new(),
            schema: self.schema.clone(),
        }
    }
}

impl PartitionStream for InformationSchemaDfSettings {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let config = self.config.clone();
        let mut builder = self.builder();
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // TODO: Stream this
            futures::stream::once(async move {
                // create a mem table with the names of tables
                config.make_df_settings(ctx.session_config().options(), &mut builder);
                Ok(builder.finish())
            }),
        ))
    }
}

struct InformationSchemaDfSettingsBuilder {
    schema: SchemaRef,
    names: StringBuilder,
    values: StringBuilder,
    descriptions: StringBuilder,
}

impl InformationSchemaDfSettingsBuilder {
    fn add_setting(&mut self, entry: ConfigEntry) {
        self.names.append_value(entry.key);
        self.values.append_option(entry.value);
        self.descriptions.append_value(entry.description);
    }

    fn finish(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(self.names.finish()),
                Arc::new(self.values.finish()),
                Arc::new(self.descriptions.finish()),
            ],
        )
        .unwrap()
    }
}
