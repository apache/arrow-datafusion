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

//! User facing options for the file formats readers

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::{plan_err, DataFusionError};

use crate::datasource::file_format::arrow::ArrowFormat;
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::parquet::ParquetFormat;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::datasource::listing::{ListingTableInsertMode, ListingTableUrl};
use crate::datasource::{
    file_format::{avro::AvroFormat, csv::CsvFormat, json::JsonFormat},
    listing::ListingOptions,
};
use crate::error::Result;
use crate::execution::context::{SessionConfig, SessionState};
use crate::logical_expr::Expr;
use datafusion_common::{
    DEFAULT_ARROW_EXTENSION, DEFAULT_AVRO_EXTENSION, DEFAULT_CSV_EXTENSION,
    DEFAULT_JSON_EXTENSION, DEFAULT_PARQUET_EXTENSION,
};

/// Options that control the reading of CSV files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional quote character. Defaults to `b'"'`.
    pub quote: u8,
    /// An optional escape character. Defaults to None.
    pub escape: Option<u8>,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::CSV.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// File compression type
    pub file_compression_type: FileCompressionType,
    /// Flag indicating whether this file may be unbounded (as in a FIFO file).
    pub infinite: bool,
    /// Indicates how the file is sorted
    pub file_sort_order: Vec<Vec<Expr>>,
    /// Setting controls how inserts to this file should be handled
    pub insert_mode: ListingTableInsertMode,
}

impl<'a> Default for CsvReadOptions<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> CsvReadOptions<'a> {
    /// Create a CSV read option with default presets
    pub fn new() -> Self {
        Self {
            has_header: true,
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            delimiter: b',',
            quote: b'"',
            escape: None,
            file_extension: DEFAULT_CSV_EXTENSION,
            table_partition_cols: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
            file_sort_order: vec![],
            insert_mode: ListingTableInsertMode::AppendToFile,
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Configure mark_infinite setting
    pub fn mark_infinite(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// Specify quote to use for CSV read
    pub fn quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn escape(mut self, escape: u8) -> Self {
        self.escape = Some(escape);
        self
    }

    /// Specify the file extension for CSV file selection
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Configure delimiter setting with Option, None value will be ignored
    pub fn delimiter_option(mut self, delimiter: Option<u8>) -> Self {
        if let Some(d) = delimiter {
            self.delimiter = d;
        }
        self
    }

    /// Specify schema to use for CSV read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }

    /// Configure file compression type
    pub fn file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Configure if file has known sort order
    pub fn file_sort_order(mut self, file_sort_order: Vec<Vec<Expr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Configure how insertions to this table should be handled
    pub fn insert_mode(mut self, insert_mode: ListingTableInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }
}

/// Options that control the reading of Parquet files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct ParquetReadOptions<'a> {
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".parquet".
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Should the parquet reader use the predicate to prune row groups?
    /// If None, uses value in SessionConfig
    pub parquet_pruning: Option<bool>,
    /// Should the parquet reader to skip any metadata that may be in
    /// the file Schema? This can help avoid schema conflicts due to
    /// metadata.
    ///
    /// If None specified, uses value in SessionConfig
    pub skip_metadata: Option<bool>,
    /// An optional schema representing the parquet files. If None, parquet reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Indicates how the file is sorted
    pub file_sort_order: Vec<Vec<Expr>>,
    /// Setting controls how inserts to this file should be handled
    pub insert_mode: ListingTableInsertMode,
}

impl<'a> Default for ParquetReadOptions<'a> {
    fn default() -> Self {
        Self {
            file_extension: DEFAULT_PARQUET_EXTENSION,
            table_partition_cols: vec![],
            parquet_pruning: None,
            skip_metadata: None,
            schema: None,
            file_sort_order: vec![],
            insert_mode: ListingTableInsertMode::AppendNewFiles,
        }
    }
}

impl<'a> ParquetReadOptions<'a> {
    /// Specify parquet_pruning
    pub fn parquet_pruning(mut self, parquet_pruning: bool) -> Self {
        self.parquet_pruning = Some(parquet_pruning);
        self
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.  Defaults to true.
    pub fn skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.skip_metadata = Some(skip_metadata);
        self
    }

    /// Specify schema to use for parquet read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Configure if file has known sort order
    pub fn file_sort_order(mut self, file_sort_order: Vec<Vec<Expr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Configure how insertions to this table should be handled
    pub fn insert_mode(mut self, insert_mode: ListingTableInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }
}

/// Options that control the reading of ARROW files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct ArrowReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<&'a Schema>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::ARROW.get_ext().as_str()`.
    pub file_extension: &'a str,

    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
}

impl<'a> Default for ArrowReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: DEFAULT_ARROW_EXTENSION,
            table_partition_cols: vec![],
        }
    }
}

impl<'a> ArrowReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Specify schema to use for AVRO read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Options that control the reading of AVRO files.
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<&'a Schema>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::AVRO.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// Flag indicating whether this file may be unbounded (as in a FIFO file).
    pub infinite: bool,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: DEFAULT_AVRO_EXTENSION,
            table_partition_cols: vec![],
            infinite: false,
        }
    }
}

impl<'a> AvroReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Configure mark_infinite setting
    pub fn mark_infinite(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }

    /// Specify schema to use for AVRO read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }
}

/// Options that control the reading of Line-delimited JSON files (NDJson)
///
/// Note this structure is supplied when a datasource is created and
/// can not not vary from statement to statement. For settings that
/// can vary statement to statement see
/// [`ConfigOptions`](crate::config::ConfigOptions).
#[derive(Clone)]
pub struct NdJsonReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from JSON files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to `FileType::JSON.get_ext().as_str()`.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<(String, DataType)>,
    /// File compression type
    pub file_compression_type: FileCompressionType,
    /// Flag indicating whether this file may be unbounded (as in a FIFO file).
    pub infinite: bool,
    /// Indicates how the file is sorted
    pub file_sort_order: Vec<Vec<Expr>>,
    /// Setting controls how inserts to this file should be handled
    pub insert_mode: ListingTableInsertMode,
}

impl<'a> Default for NdJsonReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            file_extension: DEFAULT_JSON_EXTENSION,
            table_partition_cols: vec![],
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
            file_sort_order: vec![],
            insert_mode: ListingTableInsertMode::AppendToFile,
        }
    }
}

impl<'a> NdJsonReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(
        mut self,
        table_partition_cols: Vec<(String, DataType)>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Specify file_extension
    pub fn file_extension(mut self, file_extension: &'a str) -> Self {
        self.file_extension = file_extension;
        self
    }

    /// Configure mark_infinite setting
    pub fn mark_infinite(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }

    /// Specify file_compression_type
    pub fn file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }

    /// Specify schema to use for NdJson read
    pub fn schema(mut self, schema: &'a Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Configure if file has known sort order
    pub fn file_sort_order(mut self, file_sort_order: Vec<Vec<Expr>>) -> Self {
        self.file_sort_order = file_sort_order;
        self
    }

    /// Configure how insertions to this table should be handled
    pub fn insert_mode(mut self, insert_mode: ListingTableInsertMode) -> Self {
        self.insert_mode = insert_mode;
        self
    }
}

#[async_trait]
/// ['ReadOptions'] is implemented by Options like ['CsvReadOptions'] that control the reading of respective files/sources.
pub trait ReadOptions<'a> {
    /// Helper to convert these user facing options to `ListingTable` options
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions;

    /// Infer and resolve the schema from the files/sources provided.
    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef>;

    /// helper function to reduce repetitive code. Infers the schema from sources if not provided. Infinite data sources not supported through this function.
    async fn _get_resolved_schema(
        &'a self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
        schema: Option<&'a Schema>,
        infinite: bool,
    ) -> Result<SchemaRef>
    where
        'a: 'async_trait,
    {
        match (schema, infinite) {
            (Some(s), _) => Ok(Arc::new(s.to_owned())),
            (None, false) => Ok(self
                .to_listing_options(config)
                .infer_schema(&state, &table_path)
                .await?),
            (None, true) => {
                plan_err!("Schema inference for infinite data sources is not supported.")
            }
        }
    }
}

#[async_trait]
impl ReadOptions<'_> for CsvReadOptions<'_> {
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = CsvFormat::default()
            .with_has_header(self.has_header)
            .with_delimiter(self.delimiter)
            .with_quote(self.quote)
            .with_escape(self.escape)
            .with_schema_infer_max_rec(Some(self.schema_infer_max_records))
            .with_file_compression_type(self.file_compression_type.to_owned());

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_file_sort_order(self.file_sort_order.clone())
            .with_infinite_source(self.infinite)
            .with_insert_mode(self.insert_mode.clone())
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, self.schema, self.infinite)
            .await
    }
}

#[cfg(feature = "parquet")]
#[async_trait]
impl ReadOptions<'_> for ParquetReadOptions<'_> {
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = ParquetFormat::new()
            .with_enable_pruning(self.parquet_pruning)
            .with_skip_metadata(self.skip_metadata);

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_file_sort_order(self.file_sort_order.clone())
            .with_insert_mode(self.insert_mode.clone())
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, self.schema, false)
            .await
    }
}

#[async_trait]
impl ReadOptions<'_> for NdJsonReadOptions<'_> {
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = JsonFormat::default()
            .with_schema_infer_max_rec(Some(self.schema_infer_max_records))
            .with_file_compression_type(self.file_compression_type.to_owned());

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_infinite_source(self.infinite)
            .with_file_sort_order(self.file_sort_order.clone())
            .with_insert_mode(self.insert_mode.clone())
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, self.schema, self.infinite)
            .await
    }
}

#[async_trait]
impl ReadOptions<'_> for AvroReadOptions<'_> {
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = AvroFormat;

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
            .with_infinite_source(self.infinite)
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, self.schema, self.infinite)
            .await
    }
}

#[async_trait]
impl ReadOptions<'_> for ArrowReadOptions<'_> {
    fn to_listing_options(&self, config: &SessionConfig) -> ListingOptions {
        let file_format = ArrowFormat;

        ListingOptions::new(Arc::new(file_format))
            .with_file_extension(self.file_extension)
            .with_target_partitions(config.target_partitions())
            .with_table_partition_cols(self.table_partition_cols.clone())
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef> {
        self._get_resolved_schema(config, state, table_path, self.schema, false)
            .await
    }
}
