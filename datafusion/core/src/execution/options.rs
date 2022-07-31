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

use arrow::datatypes::{Schema, SchemaRef};

use crate::datasource::file_format::parquet::ParquetFormatOptions;
use crate::datasource::file_format::DEFAULT_SCHEMA_INFER_MAX_RECORD;
use crate::datasource::{
    file_format::{
        avro::{AvroFormat, DEFAULT_AVRO_EXTENSION},
        csv::{CsvFormat, DEFAULT_CSV_EXTENSION},
        json::{JsonFormat, DEFAULT_JSON_EXTENSION},
        parquet::{ParquetFormat, DEFAULT_PARQUET_EXTENSION},
    },
    listing::ListingOptions,
};

/// CSV file read option
#[derive(Clone)]
pub struct CsvReadOptions<'a> {
    /// Does the CSV file have a header?
    ///
    /// If schema inference is run on a file with no headers, default column names
    /// are created.
    pub has_header: bool,
    /// An optional column delimiter. Defaults to `b','`.
    pub delimiter: u8,
    /// An optional schema representing the CSV files. If None, CSV reader will try to infer it
    /// based on data in file.
    pub schema: Option<&'a Schema>,
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to DEFAULT_CSV_EXTENSION.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
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
            file_extension: DEFAULT_CSV_EXTENSION,
            table_partition_cols: vec![],
        }
    }

    /// Configure has_header setting
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.has_header = has_header;
        self
    }

    /// Specify delimiter to use for CSV read
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
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
    pub fn table_partition_cols(mut self, table_partition_cols: Vec<String>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Configure number of max records to read for schema inference
    pub fn schema_infer_max_records(mut self, max_records: usize) -> Self {
        self.schema_infer_max_records = max_records;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = CsvFormat::default()
            .with_has_header(self.has_header)
            .with_delimiter(self.delimiter)
            .with_schema_infer_max_rec(Some(self.schema_infer_max_records));

        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: false,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: self.table_partition_cols.clone(),
        }
    }
}

/// Parquet read options
#[derive(Clone)]
pub struct ParquetReadOptions<'a> {
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".parquet".
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Parquet specific reading options
    pub options: ParquetFormatOptions,
}

impl<'a> Default for ParquetReadOptions<'a> {
    fn default() -> Self {
        Self {
            file_extension: DEFAULT_PARQUET_EXTENSION,
            table_partition_cols: vec![],
            options: ParquetFormatOptions::default(),
        }
    }
}

impl<'a> ParquetReadOptions<'a> {
    /// Specify parquet_pruning
    pub fn parquet_pruning(mut self, parquet_pruning: bool) -> Self {
        self.options = self.options.with_enable_pruning(parquet_pruning);
        self
    }

    /// Tell the parquet reader to skip any metadata that may be in
    /// the file Schema. This can help avoid schema conflicts due to
    /// metadata.  Defaults to true.
    pub fn skip_metadata(mut self, skip_metadata: bool) -> Self {
        self.options = self.options.with_skip_metadata(skip_metadata);
        self
    }

    /// Provide a hint to the size of the file metadata. If a hint is
    /// provided the reader will try and fetch the last `size_hint`
    /// bytes of the parquet file optimistically.  With out a hint,
    /// two read are required. One read to fetch the 8-byte parquet
    /// footer and then another read to fetch the metadata length
    /// encoded in the footer.
    pub fn metadata_size_hint(mut self, size_hint: usize) -> Self {
        self.options = self.options.with_metadata_size_hint(size_hint);
        self
    }

    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(mut self, table_partition_cols: Vec<String>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = ParquetFormat::new(self.options.clone());

        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: true,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: self.table_partition_cols.clone(),
        }
    }
}

/// Avro read options
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to DEFAULT_AVRO_EXTENSION.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: DEFAULT_AVRO_EXTENSION,
            table_partition_cols: vec![],
        }
    }
}

impl<'a> AvroReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(mut self, table_partition_cols: Vec<String>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = AvroFormat::default();

        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: false,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: self.table_partition_cols.clone(),
        }
    }
}

/// Line-delimited JSON read options
#[derive(Clone)]
pub struct NdJsonReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// Max number of rows to read from JSON files for schema inference if needed. Defaults to `DEFAULT_SCHEMA_INFER_MAX_RECORD`.
    pub schema_infer_max_records: usize,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to DEFAULT_JSON_EXTENSION.
    pub file_extension: &'a str,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
}

impl<'a> Default for NdJsonReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: DEFAULT_SCHEMA_INFER_MAX_RECORD,
            file_extension: DEFAULT_JSON_EXTENSION,
            table_partition_cols: vec![],
        }
    }
}

impl<'a> NdJsonReadOptions<'a> {
    /// Specify table_partition_cols for partition pruning
    pub fn table_partition_cols(mut self, table_partition_cols: Vec<String>) -> Self {
        self.table_partition_cols = table_partition_cols;
        self
    }

    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = JsonFormat::default();
        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: false,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: self.table_partition_cols.clone(),
        }
    }
}
