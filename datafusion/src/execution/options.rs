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

use crate::datasource::file_format::arrow_file::ArrowFormat;
use crate::datasource::file_format::json::DEFAULT_JSON_EXTENSION;
use crate::datasource::{
    file_format::{avro::AvroFormat, csv::CsvFormat},
    listing::ListingOptions,
};

/// CSV file read option
#[derive(Copy, Clone)]
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
    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,
    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".csv".
    pub file_extension: &'a str,
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
            schema_infer_max_records: 1000,
            delimiter: b',',
            file_extension: ".csv",
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
            table_partition_cols: vec![],
        }
    }
}

/// Avro read options
#[derive(Clone)]
pub struct AvroReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".avro".
    pub file_extension: &'a str,
}

impl<'a> Default for AvroReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: ".avro",
        }
    }
}

impl<'a> AvroReadOptions<'a> {
    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = AvroFormat::default();

        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: false,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: vec![],
        }
    }
}

/// Arrow read options
#[derive(Clone)]
pub struct ArrowReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".arrow".
    pub file_extension: &'a str,
}

impl<'a> Default for ArrowReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            file_extension: ".arrow",
        }
    }
}

impl<'a> ArrowReadOptions<'a> {
    /// Helper to convert these user facing options to `ListingTable` options
    pub fn to_listing_options(&self, target_partitions: usize) -> ListingOptions {
        let file_format = ArrowFormat::default();

        ListingOptions {
            format: Arc::new(file_format),
            collect_stat: false,
            file_extension: self.file_extension.to_owned(),
            target_partitions,
            table_partition_cols: vec![],
        }
    }
}

/// Line-delimited JSON read options
#[derive(Clone)]
pub struct NdJsonReadOptions<'a> {
    /// The data source schema.
    pub schema: Option<SchemaRef>,

    /// Max number of rows to read from CSV files for schema inference if needed. Defaults to 1000.
    pub schema_infer_max_records: usize,

    /// File extension; only files with this extension are selected for data input.
    /// Defaults to ".json".
    pub file_extension: &'a str,
}

impl<'a> Default for NdJsonReadOptions<'a> {
    fn default() -> Self {
        Self {
            schema: None,
            schema_infer_max_records: 1000,
            file_extension: DEFAULT_JSON_EXTENSION,
        }
    }
}
