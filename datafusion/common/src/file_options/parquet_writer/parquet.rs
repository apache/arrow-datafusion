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

//! Options related to how parquet files should be written

use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};

use crate::{config::ConfigOptions, DataFusionError, Result};

use crate::file_options::StatementOptions;
use parquet::{
    basic::{BrotliLevel, GzipLevel, ZstdLevel},
    file::properties::{EnabledStatistics, WriterVersion},
    schema::types::ColumnPath,
};

/// Options for writing parquet files
#[derive(Clone, Debug)]
pub struct ParquetWriterOptions {
    pub writer_options: WriterProperties,
}

impl ParquetWriterOptions {
    pub fn new(writer_options: WriterProperties) -> Self {
        Self { writer_options }
    }
}

impl ParquetWriterOptions {
    pub fn writer_options(&self) -> &WriterProperties {
        &self.writer_options
    }
}

/// Constructs a default Parquet WriterPropertiesBuilder using
/// Session level ConfigOptions to initialize settings
pub fn default_builder(options: &ConfigOptions) -> Result<WriterPropertiesBuilder> {
    let parquet_session_options = &options.execution.parquet;
    let mut builder = WriterProperties::builder()
        .set_data_page_size_limit(parquet_session_options.data_pagesize_limit)
        .set_write_batch_size(parquet_session_options.write_batch_size)
        .set_writer_version(parse_version_string(
            &parquet_session_options.writer_version,
        )?)
        .set_dictionary_page_size_limit(
            parquet_session_options.dictionary_page_size_limit,
        )
        .set_max_row_group_size(parquet_session_options.max_row_group_size)
        .set_created_by(parquet_session_options.created_by.clone())
        .set_column_index_truncate_length(
            parquet_session_options.column_index_truncate_length,
        )
        .set_data_page_row_count_limit(parquet_session_options.data_page_row_count_limit)
        .set_bloom_filter_enabled(parquet_session_options.bloom_filter_enabled);

    builder = match &parquet_session_options.encoding {
        Some(encoding) => builder.set_encoding(parse_encoding_string(encoding)?),
        None => builder,
    };

    builder = match &parquet_session_options.dictionary_enabled {
        Some(enabled) => builder.set_dictionary_enabled(*enabled),
        None => builder,
    };

    builder = match &parquet_session_options.compression {
        Some(compression) => {
            builder.set_compression(parse_compression_string(compression)?)
        }
        None => builder,
    };

    builder = match &parquet_session_options.statistics_enabled {
        Some(statistics) => {
            builder.set_statistics_enabled(parse_statistics_string(statistics)?)
        }
        None => builder,
    };

    builder = match &parquet_session_options.max_statistics_size {
        Some(size) => builder.set_max_statistics_size(*size),
        None => builder,
    };

    builder = match &parquet_session_options.bloom_filter_fpp {
        Some(fpp) => builder.set_bloom_filter_fpp(*fpp),
        None => builder,
    };

    builder = match &parquet_session_options.bloom_filter_ndv {
        Some(ndv) => builder.set_bloom_filter_ndv(*ndv),
        None => builder,
    };

    Ok(builder)
}

impl TryFrom<(&ConfigOptions, &StatementOptions)> for ParquetWriterOptions {
    type Error = DataFusionError;

    fn try_from(
        configs_and_statement_options: (&ConfigOptions, &StatementOptions),
    ) -> Result<Self> {
        let configs = configs_and_statement_options.0;
        let statement_options = configs_and_statement_options.1;
        let mut builder = default_builder(configs)?;
        for (option, value) in &statement_options.options {
            let (option, col_path) = split_option_and_column_path(option);
            builder = match option.to_lowercase().as_str(){
                "max_row_group_size" => builder
                    .set_max_row_group_size(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as u64 as required for {option}!")))?),
                "data_pagesize_limit" => builder
                    .set_data_page_size_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "write_batch_size" => builder
                    .set_write_batch_size(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "writer_version" => builder
                    .set_writer_version(parse_version_string(value)?),
                "dictionary_page_size_limit" => builder
                    .set_dictionary_page_size_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "created_by" => builder
                    .set_created_by(value.to_owned()),
                "column_index_truncate_length" => builder
                    .set_column_index_truncate_length(Some(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?)),
                "data_page_row_count_limit" => builder
                    .set_data_page_row_count_limit(value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?),
                "bloom_filter_enabled" => {
                    let parsed_value = value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as bool as required for {option}!")))?;
                    match col_path{
                        Some(path) => builder.set_column_bloom_filter_enabled(path, parsed_value),
                        None => builder.set_bloom_filter_enabled(parsed_value)
                    }
                },
                "encoding" => {
                    let parsed_encoding = parse_encoding_string(value)?;
                    match col_path{
                        Some(path) => builder.set_column_encoding(path, parsed_encoding),
                        None => builder.set_encoding(parsed_encoding)
                    }
                },
                "dictionary_enabled" => {
                    let parsed_value = value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as bool as required for {option}!")))?;
                    match col_path{
                        Some(path) => builder.set_column_dictionary_enabled(path, parsed_value),
                        None => builder.set_dictionary_enabled(parsed_value)
                    }
                },
                "compression" => {
                    let parsed_compression = parse_compression_string(value)?;
                    match col_path{
                        Some(path) => builder.set_column_compression(path, parsed_compression),
                        None => builder.set_compression(parsed_compression)
                    }
                },
                "statistics_enabled" => {
                    let parsed_value = parse_statistics_string(value)?;
                    match col_path{
                        Some(path) => builder.set_column_statistics_enabled(path, parsed_value),
                        None => builder.set_statistics_enabled(parsed_value)
                    }
                },
                "max_statistics_size" => {
                    let parsed_value = value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as usize as required for {option}!")))?;
                    match col_path{
                        Some(path) => builder.set_column_max_statistics_size(path, parsed_value),
                        None => builder.set_max_statistics_size(parsed_value)
                    }
                },
                "bloom_filter_fpp" => {
                    let parsed_value = value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as f64 as required for {option}!")))?;
                    match col_path{
                        Some(path) => builder.set_column_bloom_filter_fpp(path, parsed_value),
                        None => builder.set_bloom_filter_fpp(parsed_value)
                    }
                },
                "bloom_filter_ndv" => {
                    let parsed_value = value.parse()
                    .map_err(|_| DataFusionError::Configuration(format!("Unable to parse {value} as u64 as required for {option}!")))?;
                    match col_path{
                        Some(path) => builder.set_column_bloom_filter_ndv(path, parsed_value),
                        None => builder.set_bloom_filter_ndv(parsed_value)
                    }
                },
                _ => return Err(DataFusionError::Configuration(format!("Found unsupported option {option} with value {value} for Parquet format!")))
            }
        }
        Ok(ParquetWriterOptions {
            writer_options: builder.build(),
        })
    }
}

/// Parses datafusion.execution.parquet.encoding String to a parquet::basic::Encoding
pub(crate) fn parse_encoding_string(
    str_setting: &str,
) -> Result<parquet::basic::Encoding> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "plain" => Ok(parquet::basic::Encoding::PLAIN),
        "plain_dictionary" => Ok(parquet::basic::Encoding::PLAIN_DICTIONARY),
        "rle" => Ok(parquet::basic::Encoding::RLE),
        "bit_packed" => Ok(parquet::basic::Encoding::BIT_PACKED),
        "delta_binary_packed" => Ok(parquet::basic::Encoding::DELTA_BINARY_PACKED),
        "delta_length_byte_array" => {
            Ok(parquet::basic::Encoding::DELTA_LENGTH_BYTE_ARRAY)
        }
        "delta_byte_array" => Ok(parquet::basic::Encoding::DELTA_BYTE_ARRAY),
        "rle_dictionary" => Ok(parquet::basic::Encoding::RLE_DICTIONARY),
        "byte_stream_split" => Ok(parquet::basic::Encoding::BYTE_STREAM_SPLIT),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet encoding: \
        {str_setting}. Valid values are: plain, plain_dictionary, rle, \
        bit_packed, delta_binary_packed, delta_length_byte_array, \
        delta_byte_array, rle_dictionary, and byte_stream_split."
        ))),
    }
}

/// Splits compression string into compression codec and optional compression_level
/// I.e. gzip(2) -> gzip, 2
fn split_compression_string(str_setting: &str) -> Result<(String, Option<u32>)> {
    // ignore string literal chars passed from sqlparser i.e. remove single quotes
    let str_setting = str_setting.replace('\'', "");
    let split_setting = str_setting.split_once('(');

    match split_setting {
        Some((codec, rh)) => {
            let level = &rh[..rh.len() - 1].parse::<u32>().map_err(|_| {
                DataFusionError::Configuration(format!(
                    "Could not parse compression string. \
                    Got codec: {} and unknown level from {}",
                    codec, str_setting
                ))
            })?;
            Ok((codec.to_owned(), Some(*level)))
        }
        None => Ok((str_setting.to_owned(), None)),
    }
}

/// Helper to ensure compression codecs which don't support levels
/// don't have one set. E.g. snappy(2) is invalid.
fn check_level_is_none(codec: &str, level: &Option<u32>) -> Result<()> {
    if level.is_some() {
        return Err(DataFusionError::Configuration(format!(
            "Compression {codec} does not support specifying a level"
        )));
    }
    Ok(())
}

/// Helper to ensure compression codecs which require a level
/// do have one set. E.g. zstd is invalid, zstd(3) is valid
fn require_level(codec: &str, level: Option<u32>) -> Result<u32> {
    level.ok_or(DataFusionError::Configuration(format!(
        "{codec} compression requires specifying a level such as {codec}(4)"
    )))
}

/// Parses datafusion.execution.parquet.compression String to a parquet::basic::Compression
pub(crate) fn parse_compression_string(
    str_setting: &str,
) -> Result<parquet::basic::Compression> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    let (codec, level) = split_compression_string(str_setting_lower)?;
    let codec = codec.as_str();
    match codec {
        "uncompressed" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::UNCOMPRESSED)
        }
        "snappy" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::SNAPPY)
        }
        "gzip" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::GZIP(GzipLevel::try_new(
                level,
            )?))
        }
        "lzo" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZO)
        }
        "brotli" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::BROTLI(BrotliLevel::try_new(
                level,
            )?))
        }
        "lz4" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZ4)
        }
        "zstd" => {
            let level = require_level(codec, level)?;
            Ok(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(
                level as i32,
            )?))
        }
        "lz4_raw" => {
            check_level_is_none(codec, &level)?;
            Ok(parquet::basic::Compression::LZ4_RAW)
        }
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet compression: \
        {str_setting}. Valid values are: uncompressed, snappy, gzip(level), \
        lzo, brotli(level), lz4, zstd(level), and lz4_raw."
        ))),
    }
}

pub(crate) fn parse_version_string(str_setting: &str) -> Result<WriterVersion> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "1.0" => Ok(WriterVersion::PARQUET_1_0),
        "2.0" => Ok(WriterVersion::PARQUET_2_0),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet writer version {str_setting} \
            valid options are 1.0 and 2.0"
        ))),
    }
}

pub(crate) fn parse_statistics_string(str_setting: &str) -> Result<EnabledStatistics> {
    let str_setting_lower: &str = &str_setting.to_lowercase();
    match str_setting_lower {
        "none" => Ok(EnabledStatistics::None),
        "chunk" => Ok(EnabledStatistics::Chunk),
        "page" => Ok(EnabledStatistics::Page),
        _ => Err(DataFusionError::Configuration(format!(
            "Unknown or unsupported parquet statistics setting {str_setting} \
            valid options are none, page, and chunk"
        ))),
    }
}

pub(crate) fn split_option_and_column_path(
    str_setting: &str,
) -> (String, Option<ColumnPath>) {
    match str_setting.replace('\'', "").split_once("::") {
        Some((s1, s2)) => {
            let col_path = ColumnPath::new(s2.split('.').map(|s| s.to_owned()).collect());
            (s1.to_owned(), Some(col_path))
        }
        None => (str_setting.to_owned(), None),
    }
}
