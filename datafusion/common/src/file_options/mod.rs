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

//! Options related to how files should be written

pub mod arrow_writer;
pub mod avro_writer;
pub mod csv_writer;
pub mod file_type;
pub mod json_writer;
pub mod parquet_writer;
pub(crate) mod parse_utils;

use std::{
    collections::HashMap,
    fmt::{self, Display},
    path::Path,
    str::FromStr,
};

use crate::{
    config::ConfigOptions, file_options::parse_utils::parse_boolean_string,
    DataFusionError, FileType, Result,
};

use self::{
    arrow_writer::ArrowWriterOptions, avro_writer::AvroWriterOptions,
    csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions,
};

use self::parquet_writer::ParquetWriterOptions;

/// Represents a single arbitrary setting in a
/// [StatementOptions] where OptionTuple.0 determines
/// the specific setting to be modified and OptionTuple.1
/// determines the value which should be applied
pub type OptionTuple = (String, String);

/// Represents arbitrary tuples of options passed as String
/// tuples from SQL statements. As in the following statement:
/// COPY ... TO ... (setting1 value1, setting2 value2, ...)
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct StatementOptions {
    options: Vec<OptionTuple>,
}

/// Useful for conversion from external tables which use Hashmap<String, String>
impl From<&HashMap<String, String>> for StatementOptions {
    fn from(value: &HashMap<String, String>) -> Self {
        Self {
            options: value
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect::<Vec<OptionTuple>>(),
        }
    }
}

impl StatementOptions {
    pub fn new(options: Vec<OptionTuple>) -> Self {
        Self { options }
    }

    pub fn into_inner(self) -> Vec<OptionTuple> {
        self.options
    }

    /// Scans for option and if it exists removes it and attempts to parse as a boolean
    /// Returns none if it does not exist.
    pub fn take_bool_option(&mut self, find: &str) -> Result<Option<bool>> {
        let maybe_option = self.scan_and_remove_option(find);
        maybe_option
            .map(|(_, v)| parse_boolean_string(find, v))
            .transpose()
    }

    /// Scans for option and if it exists removes it and returns it
    /// Returns none if it does not exist
    pub fn take_str_option(&mut self, find: &str) -> Option<String> {
        let maybe_option = self.scan_and_remove_option(find);
        maybe_option.map(|(_, v)| v)
    }

    /// Infers the file_type given a target and arbitrary options.
    /// If the options contain an explicit "format" option, that will be used.
    /// Otherwise, attempt to infer file_type from the extension of target.
    /// Finally, return an error if unable to determine the file_type
    /// If found, format is removed from the options list.
    pub fn try_infer_file_type(&mut self, target: &str) -> Result<FileType> {
        let explicit_format = self.scan_and_remove_option("format");
        let format = match explicit_format {
            Some(s) => FileType::from_str(s.1.as_str()),
            None => {
                // try to infer file format from file extension
                let extension: &str = &Path::new(target)
                    .extension()
                    .ok_or(DataFusionError::Configuration(
                        "Format not explicitly set and unable to get file extension!"
                            .to_string(),
                    ))?
                    .to_str()
                    .ok_or(DataFusionError::Configuration(
                        "Format not explicitly set and failed to parse file extension!"
                            .to_string(),
                    ))?
                    .to_lowercase();

                FileType::from_str(extension)
            }
        }?;

        Ok(format)
    }

    /// Finds an option in StatementOptions if exists, removes and returns it
    /// along with the vec of remaining options.
    fn scan_and_remove_option(&mut self, find: &str) -> Option<OptionTuple> {
        let idx = self
            .options
            .iter()
            .position(|(k, _)| k.to_lowercase() == find.to_lowercase());
        match idx {
            Some(i) => Some(self.options.swap_remove(i)),
            None => None,
        }
    }
}

/// This type contains all options needed to initialize a particular
/// RecordBatchWriter type. Each element in the enum contains a thin wrapper
/// around a "writer builder" type (e.g. arrow::csv::WriterBuilder)
/// plus any DataFusion specific writing options (e.g. CSV compression)
#[derive(Clone, Debug)]
pub enum FileTypeWriterOptions {
    Parquet(ParquetWriterOptions),
    CSV(CsvWriterOptions),
    JSON(JsonWriterOptions),
    Avro(AvroWriterOptions),
    Arrow(ArrowWriterOptions),
}

impl FileTypeWriterOptions {
    /// Constructs a FileTypeWriterOptions given a FileType to be written
    /// and arbitrary String tuple options. May return an error if any
    /// string setting is unrecognized or unsupported.
    pub fn build(
        file_type: &FileType,
        config_defaults: &ConfigOptions,
        statement_options: &StatementOptions,
    ) -> Result<Self> {
        let options = (config_defaults, statement_options);

        let file_type_write_options = match file_type {
            FileType::PARQUET => {
                FileTypeWriterOptions::Parquet(ParquetWriterOptions::try_from(options)?)
            }
            FileType::CSV => {
                FileTypeWriterOptions::CSV(CsvWriterOptions::try_from(options)?)
            }
            FileType::JSON => {
                FileTypeWriterOptions::JSON(JsonWriterOptions::try_from(options)?)
            }
            FileType::AVRO => {
                FileTypeWriterOptions::Avro(AvroWriterOptions::try_from(options)?)
            }
            FileType::ARROW => {
                FileTypeWriterOptions::Arrow(ArrowWriterOptions::try_from(options)?)
            }
        };

        Ok(file_type_write_options)
    }

    /// Constructs a FileTypeWriterOptions from session defaults only.
    pub fn build_default(
        file_type: &FileType,
        config_defaults: &ConfigOptions,
    ) -> Result<Self> {
        let empty_statement = StatementOptions::new(vec![]);
        let options = (config_defaults, &empty_statement);

        let file_type_write_options = match file_type {
            FileType::PARQUET => {
                FileTypeWriterOptions::Parquet(ParquetWriterOptions::try_from(options)?)
            }
            FileType::CSV => {
                FileTypeWriterOptions::CSV(CsvWriterOptions::try_from(options)?)
            }
            FileType::JSON => {
                FileTypeWriterOptions::JSON(JsonWriterOptions::try_from(options)?)
            }
            FileType::AVRO => {
                FileTypeWriterOptions::Avro(AvroWriterOptions::try_from(options)?)
            }
            FileType::ARROW => {
                FileTypeWriterOptions::Arrow(ArrowWriterOptions::try_from(options)?)
            }
        };

        Ok(file_type_write_options)
    }

    /// Tries to extract ParquetWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from parquet is set.
    pub fn try_into_parquet(&self) -> Result<&ParquetWriterOptions> {
        match self {
            FileTypeWriterOptions::Parquet(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(format!(
                "Expected parquet options but found options for: {}",
                self
            ))),
        }
    }

    /// Tries to extract CsvWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from csv is set.
    pub fn try_into_csv(&self) -> Result<&CsvWriterOptions> {
        match self {
            FileTypeWriterOptions::CSV(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(format!(
                "Expected csv options but found options for {}",
                self
            ))),
        }
    }

    /// Tries to extract JsonWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from json is set.
    pub fn try_into_json(&self) -> Result<&JsonWriterOptions> {
        match self {
            FileTypeWriterOptions::JSON(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(format!(
                "Expected json options but found options for {}",
                self,
            ))),
        }
    }

    /// Tries to extract AvroWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from avro is set.
    pub fn try_into_avro(&self) -> Result<&AvroWriterOptions> {
        match self {
            FileTypeWriterOptions::Avro(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(format!(
                "Expected avro options but found options for {}!",
                self
            ))),
        }
    }

    /// Tries to extract ArrowWriterOptions from this FileTypeWriterOptions enum.
    /// Returns an error if a different type from arrow is set.
    pub fn try_into_arrow(&self) -> Result<&ArrowWriterOptions> {
        match self {
            FileTypeWriterOptions::Arrow(opt) => Ok(opt),
            _ => Err(DataFusionError::Internal(format!(
                "Expected arrow options but found options for {}",
                self
            ))),
        }
    }
}

impl Display for FileTypeWriterOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            FileTypeWriterOptions::Arrow(_) => "ArrowWriterOptions",
            FileTypeWriterOptions::Avro(_) => "AvroWriterOptions",
            FileTypeWriterOptions::CSV(_) => "CsvWriterOptions",
            FileTypeWriterOptions::JSON(_) => "JsonWriterOptions",
            FileTypeWriterOptions::Parquet(_) => "ParquetWriterOptions",
        };
        write!(f, "{}", name)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use parquet::{
        basic::{Compression, Encoding, ZstdLevel},
        file::properties::{EnabledStatistics, WriterVersion},
        schema::types::ColumnPath,
    };

    use crate::{
        config::ConfigOptions,
        file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
        parsers::CompressionTypeVariant,
    };

    use crate::Result;

    use super::{parquet_writer::ParquetWriterOptions, StatementOptions};

    #[test]
    fn test_writeroptions_parquet_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("max_row_group_size".to_owned(), "123".to_owned());
        option_map.insert("data_pagesize_limit".to_owned(), "123".to_owned());
        option_map.insert("write_batch_size".to_owned(), "123".to_owned());
        option_map.insert("writer_version".to_owned(), "2.0".to_owned());
        option_map.insert("dictionary_page_size_limit".to_owned(), "123".to_owned());
        option_map.insert("created_by".to_owned(), "df write unit test".to_owned());
        option_map.insert("column_index_truncate_length".to_owned(), "123".to_owned());
        option_map.insert("data_page_row_count_limit".to_owned(), "123".to_owned());
        option_map.insert("bloom_filter_enabled".to_owned(), "true".to_owned());
        option_map.insert("encoding".to_owned(), "plain".to_owned());
        option_map.insert("dictionary_enabled".to_owned(), "true".to_owned());
        option_map.insert("compression".to_owned(), "zstd(4)".to_owned());
        option_map.insert("statistics_enabled".to_owned(), "page".to_owned());
        option_map.insert("bloom_filter_fpp".to_owned(), "0.123".to_owned());
        option_map.insert("bloom_filter_ndv".to_owned(), "123".to_owned());

        let options = StatementOptions::from(&option_map);
        let config = ConfigOptions::new();

        let parquet_options = ParquetWriterOptions::try_from((&config, &options))?;
        let properties = parquet_options.writer_options();

        // Verify the expected options propagated down to parquet crate WriterProperties struct
        assert_eq!(properties.max_row_group_size(), 123);
        assert_eq!(properties.data_page_size_limit(), 123);
        assert_eq!(properties.write_batch_size(), 123);
        assert_eq!(properties.writer_version(), WriterVersion::PARQUET_2_0);
        assert_eq!(properties.dictionary_page_size_limit(), 123);
        assert_eq!(properties.created_by(), "df write unit test");
        assert_eq!(properties.column_index_truncate_length(), Some(123));
        assert_eq!(properties.data_page_row_count_limit(), 123);
        properties
            .bloom_filter_properties(&ColumnPath::from(""))
            .expect("expected bloom filter enabled");
        assert_eq!(
            properties
                .encoding(&ColumnPath::from(""))
                .expect("expected default encoding"),
            Encoding::PLAIN
        );
        assert!(properties.dictionary_enabled(&ColumnPath::from("")));
        assert_eq!(
            properties.compression(&ColumnPath::from("")),
            Compression::ZSTD(ZstdLevel::try_new(4_i32)?)
        );
        assert_eq!(
            properties.statistics_enabled(&ColumnPath::from("")),
            EnabledStatistics::Page
        );
        assert_eq!(
            properties
                .bloom_filter_properties(&ColumnPath::from(""))
                .expect("expected bloom properties!")
                .fpp,
            0.123
        );
        assert_eq!(
            properties
                .bloom_filter_properties(&ColumnPath::from(""))
                .expect("expected bloom properties!")
                .ndv,
            123
        );

        Ok(())
    }

    #[test]
    fn test_writeroptions_parquet_column_specific() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();

        option_map.insert("bloom_filter_enabled::col1".to_owned(), "true".to_owned());
        option_map.insert(
            "bloom_filter_enabled::col2.nested".to_owned(),
            "true".to_owned(),
        );
        option_map.insert("encoding::col1".to_owned(), "plain".to_owned());
        option_map.insert("encoding::col2.nested".to_owned(), "rle".to_owned());
        option_map.insert("dictionary_enabled::col1".to_owned(), "true".to_owned());
        option_map.insert(
            "dictionary_enabled::col2.nested".to_owned(),
            "true".to_owned(),
        );
        option_map.insert("compression::col1".to_owned(), "zstd(4)".to_owned());
        option_map.insert("compression::col2.nested".to_owned(), "zstd(10)".to_owned());
        option_map.insert("statistics_enabled::col1".to_owned(), "page".to_owned());
        option_map.insert(
            "statistics_enabled::col2.nested".to_owned(),
            "none".to_owned(),
        );
        option_map.insert("bloom_filter_fpp::col1".to_owned(), "0.123".to_owned());
        option_map.insert(
            "bloom_filter_fpp::col2.nested".to_owned(),
            "0.456".to_owned(),
        );
        option_map.insert("bloom_filter_ndv::col1".to_owned(), "123".to_owned());
        option_map.insert("bloom_filter_ndv::col2.nested".to_owned(), "456".to_owned());

        let options = StatementOptions::from(&option_map);
        let config = ConfigOptions::new();

        let parquet_options = ParquetWriterOptions::try_from((&config, &options))?;
        let properties = parquet_options.writer_options();

        let col1 = ColumnPath::from(vec!["col1".to_owned()]);
        let col2_nested = ColumnPath::from(vec!["col2".to_owned(), "nested".to_owned()]);

        // Verify the expected options propagated down to parquet crate WriterProperties struct

        properties
            .bloom_filter_properties(&col1)
            .expect("expected bloom filter enabled for col1");

        properties
            .bloom_filter_properties(&col2_nested)
            .expect("expected bloom filter enabled cor col2_nested");

        assert_eq!(
            properties.encoding(&col1).expect("expected encoding"),
            Encoding::PLAIN
        );

        assert_eq!(
            properties
                .encoding(&col2_nested)
                .expect("expected encoding"),
            Encoding::RLE
        );

        assert!(properties.dictionary_enabled(&col1));
        assert!(properties.dictionary_enabled(&col2_nested));

        assert_eq!(
            properties.compression(&col1),
            Compression::ZSTD(ZstdLevel::try_new(4_i32)?)
        );

        assert_eq!(
            properties.compression(&col2_nested),
            Compression::ZSTD(ZstdLevel::try_new(10_i32)?)
        );

        assert_eq!(
            properties.statistics_enabled(&col1),
            EnabledStatistics::Page
        );

        assert_eq!(
            properties.statistics_enabled(&col2_nested),
            EnabledStatistics::None
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col1)
                .expect("expected bloom properties!")
                .fpp,
            0.123
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col2_nested)
                .expect("expected bloom properties!")
                .fpp,
            0.456
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col1)
                .expect("expected bloom properties!")
                .ndv,
            123
        );

        assert_eq!(
            properties
                .bloom_filter_properties(&col2_nested)
                .expect("expected bloom properties!")
                .ndv,
            456
        );

        Ok(())
    }

    #[test]
    fn test_writeroptions_csv_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("header".to_owned(), "true".to_owned());
        option_map.insert("date_format".to_owned(), "123".to_owned());
        option_map.insert("datetime_format".to_owned(), "123".to_owned());
        option_map.insert("timestamp_format".to_owned(), "2.0".to_owned());
        option_map.insert("time_format".to_owned(), "123".to_owned());
        option_map.insert("rfc3339".to_owned(), "true".to_owned());
        option_map.insert("null_value".to_owned(), "123".to_owned());
        option_map.insert("compression".to_owned(), "gzip".to_owned());
        option_map.insert("delimiter".to_owned(), ";".to_owned());

        let options = StatementOptions::from(&option_map);
        let config = ConfigOptions::new();

        let csv_options = CsvWriterOptions::try_from((&config, &options))?;
        let builder = csv_options.writer_options;
        assert!(builder.header());
        let buff = Vec::new();
        let _properties = builder.build(buff);
        assert_eq!(csv_options.compression, CompressionTypeVariant::GZIP);
        // TODO expand unit test if csv::WriterBuilder allows public read access to properties

        Ok(())
    }

    #[test]
    fn test_writeroptions_json_from_statement_options() -> Result<()> {
        let mut option_map: HashMap<String, String> = HashMap::new();
        option_map.insert("compression".to_owned(), "gzip".to_owned());

        let options = StatementOptions::from(&option_map);
        let config = ConfigOptions::new();

        let json_options = JsonWriterOptions::try_from((&config, &options))?;
        assert_eq!(json_options.compression, CompressionTypeVariant::GZIP);

        Ok(())
    }
}
