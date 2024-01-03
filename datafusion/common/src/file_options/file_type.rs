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

//! File type abstraction

use crate::error::{DataFusionError, Result};
use crate::parsers::CompressionTypeVariant;

use core::fmt;
use std::fmt::Display;
use std::hash::Hasher;
use std::str::FromStr;

/// The default file extension of arrow files
pub const DEFAULT_ARROW_EXTENSION: &str = ".arrow";
/// The default file extension of avro files
pub const DEFAULT_AVRO_EXTENSION: &str = ".avro";
/// The default file extension of csv files
pub const DEFAULT_CSV_EXTENSION: &str = ".csv";
/// The default file extension of json files
pub const DEFAULT_JSON_EXTENSION: &str = ".json";
/// The default file extension of parquet files
pub const DEFAULT_PARQUET_EXTENSION: &str = ".parquet";

/// Define each `FileType`/`FileCompressionType`'s extension
pub trait GetExt {
    /// File extension getter
    fn get_ext(&self) -> String;
}

/// Readable file type
#[allow(clippy::derived_hash_with_manual_eq)]
#[derive(Debug, Clone, Hash)]
pub enum FileType {
    /// Apache Arrow file
    ARROW,
    /// Apache Avro file
    AVRO,
    /// Apache Parquet file
    #[cfg(feature = "parquet")]
    PARQUET,
    /// CSV file
    CSV,
    /// JSON file
    JSON,
    /// FileType Implemented Outside of DataFusion
    Extension(Box<dyn ExtensionFileType>),
}

/// A trait to enable externally implementing the functionality of a [FileType].
pub trait ExtensionFileType:
    std::fmt::Debug + ExtensionFileTypeClone + Send + Sync
{
    /// Returns the default file extension for this type, e.g. CSV would return ".csv".to_owned()
    /// The default_extension is also used to uniquely identify a specific FileType::Extension variant,
    /// so ensure this String is unique from any built in FileType and any other ExtensionFileTypes
    /// defined.
    fn default_extension(&self) -> String;

    /// Returns the file extension when it is compressed with a given [CompressionTypeVariant]
    fn extension_with_compression(
        &self,
        compression: CompressionTypeVariant,
    ) -> Result<String>;
}

pub trait ExtensionFileTypeClone {
    fn clone_box(&self) -> Box<dyn ExtensionFileType>;
}

impl Clone for Box<dyn ExtensionFileType> {
    fn clone(&self) -> Box<dyn ExtensionFileType> {
        self.clone_box()
    }
}

impl std::hash::Hash for Box<dyn ExtensionFileType> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.default_extension().hash(state)
    }
}

impl PartialEq for FileType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FileType::ARROW, FileType::ARROW) => true,
            (FileType::AVRO, FileType::AVRO) => true,
            #[cfg(feature = "parquet")]
            (FileType::PARQUET, FileType::PARQUET) => true,
            (FileType::CSV, FileType::CSV) => true,
            (FileType::JSON, FileType::JSON) => true,
            (FileType::Extension(ext_self), FileType::Extension(ext_other)) => {
                ext_self.default_extension() == ext_other.default_extension()
            }
            _ => false,
        }
    }
}

impl Eq for FileType {}

impl GetExt for FileType {
    fn get_ext(&self) -> String {
        match self {
            FileType::ARROW => DEFAULT_ARROW_EXTENSION.to_owned(),
            FileType::AVRO => DEFAULT_AVRO_EXTENSION.to_owned(),
            #[cfg(feature = "parquet")]
            FileType::PARQUET => DEFAULT_PARQUET_EXTENSION.to_owned(),
            FileType::CSV => DEFAULT_CSV_EXTENSION.to_owned(),
            FileType::JSON => DEFAULT_JSON_EXTENSION.to_owned(),
            FileType::Extension(ext) => ext.default_extension(),
        }
    }
}

impl Display for FileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            FileType::CSV => "csv",
            FileType::JSON => "json",
            #[cfg(feature = "parquet")]
            FileType::PARQUET => "parquet",
            FileType::AVRO => "avro",
            FileType::ARROW => "arrow",
            FileType::Extension(ext) => return ext.fmt(f),
        };
        write!(f, "{}", out)
    }
}

impl FromStr for FileType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "ARROW" => Ok(FileType::ARROW),
            "AVRO" => Ok(FileType::AVRO),
            #[cfg(feature = "parquet")]
            "PARQUET" => Ok(FileType::PARQUET),
            "CSV" => Ok(FileType::CSV),
            "JSON" | "NDJSON" => Ok(FileType::JSON),
            _ => Err(DataFusionError::NotImplemented(format!(
                "Unknown FileType: {s}"
            ))),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "parquet")]
mod tests {
    use crate::error::DataFusionError;
    use crate::file_options::FileType;
    use std::str::FromStr;

    #[test]
    fn from_str() {
        for (ext, file_type) in [
            ("csv", FileType::CSV),
            ("CSV", FileType::CSV),
            ("json", FileType::JSON),
            ("JSON", FileType::JSON),
            ("avro", FileType::AVRO),
            ("AVRO", FileType::AVRO),
            ("parquet", FileType::PARQUET),
            ("PARQUET", FileType::PARQUET),
        ] {
            assert_eq!(FileType::from_str(ext).unwrap(), file_type);
        }

        assert!(matches!(
            FileType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
}
