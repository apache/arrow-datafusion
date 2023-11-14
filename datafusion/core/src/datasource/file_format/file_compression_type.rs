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

//! File Compression type abstraction

use crate::error::{DataFusionError, Result};
#[cfg(feature = "compression")]
use async_compression::tokio::bufread::{
    BzDecoder as AsyncBzDecoder, BzEncoder as AsyncBzEncoder,
    GzipDecoder as AsyncGzDecoder, GzipEncoder as AsyncGzEncoder,
    XzDecoder as AsyncXzDecoder, XzEncoder as AsyncXzEncoder,
    ZstdDecoder as AsyncZstdDecoer, ZstdEncoder as AsyncZstdEncoder,
};

#[cfg(feature = "compression")]
use async_compression::tokio::write::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
use bytes::Bytes;
#[cfg(feature = "compression")]
use bzip2::read::MultiBzDecoder;
use datafusion_common::{parsers::CompressionTypeVariant, FileType, GetExt};
#[cfg(feature = "compression")]
use flate2::read::MultiGzDecoder;

use futures::stream::BoxStream;
use futures::StreamExt;
#[cfg(feature = "compression")]
use futures::TryStreamExt;
use std::str::FromStr;
use tokio::io::AsyncWrite;
#[cfg(feature = "compression")]
use tokio_util::io::{ReaderStream, StreamReader};
#[cfg(feature = "compression")]
use xz2::read::XzDecoder;
#[cfg(feature = "compression")]
use zstd::Decoder as ZstdDecoder;
use CompressionTypeVariant::*;

/// Readable file compression type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileCompressionType {
    variant: CompressionTypeVariant,
}

impl GetExt for FileCompressionType {
    fn get_ext(&self) -> String {
        match self.variant {
            GZIP => ".gz".to_owned(),
            BZIP2 => ".bz2".to_owned(),
            XZ => ".xz".to_owned(),
            ZSTD => ".zst".to_owned(),
            UNCOMPRESSED => "".to_owned(),
        }
    }
}

impl From<CompressionTypeVariant> for FileCompressionType {
    fn from(t: CompressionTypeVariant) -> Self {
        Self { variant: t }
    }
}

impl FromStr for FileCompressionType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let variant = CompressionTypeVariant::from_str(s).map_err(|_| {
            DataFusionError::NotImplemented(format!("Unknown FileCompressionType: {s}"))
        })?;
        Ok(Self { variant })
    }
}

/// `FileCompressionType` implementation
impl FileCompressionType {
    /// Gzip-ed file
    pub const GZIP: Self = Self { variant: GZIP };

    /// Bzip2-ed file
    pub const BZIP2: Self = Self { variant: BZIP2 };

    /// Xz-ed file (liblzma)
    pub const XZ: Self = Self { variant: XZ };

    /// Zstd-ed file
    pub const ZSTD: Self = Self { variant: ZSTD };

    /// Uncompressed file
    pub const UNCOMPRESSED: Self = Self {
        variant: UNCOMPRESSED,
    };

    /// The file is compressed or not
    pub const fn is_compressed(&self) -> bool {
        self.variant.is_compressed()
    }

    /// Given a `Stream`, create a `Stream` which data are compressed with `FileCompressionType`.
    pub fn convert_to_compress_stream(
        &self,
        s: BoxStream<'static, Result<Bytes>>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => ReaderStream::new(AsyncGzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            BZIP2 => ReaderStream::new(AsyncBzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            XZ => ReaderStream::new(AsyncXzEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            ZSTD => ReaderStream::new(AsyncZstdEncoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => s.boxed(),
        })
    }

    /// Wrap the given `AsyncWrite` so that it performs compressed writes
    /// according to this `FileCompressionType`.
    pub fn convert_async_writer(
        &self,
        w: Box<dyn AsyncWrite + Send + Unpin>,
    ) -> Result<Box<dyn AsyncWrite + Send + Unpin>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => Box::new(GzipEncoder::new(w)),
            #[cfg(feature = "compression")]
            BZIP2 => Box::new(BzEncoder::new(w)),
            #[cfg(feature = "compression")]
            XZ => Box::new(XzEncoder::new(w)),
            #[cfg(feature = "compression")]
            ZSTD => Box::new(ZstdEncoder::new(w)),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => w,
        })
    }

    /// Given a `Stream`, create a `Stream` which data are decompressed with `FileCompressionType`.
    pub fn convert_stream(
        &self,
        s: BoxStream<'static, Result<Bytes>>,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => ReaderStream::new(AsyncGzDecoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            BZIP2 => ReaderStream::new(AsyncBzDecoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            XZ => ReaderStream::new(AsyncXzDecoder::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(feature = "compression")]
            ZSTD => ReaderStream::new(AsyncZstdDecoer::new(StreamReader::new(s)))
                .map_err(DataFusionError::from)
                .boxed(),
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => s.boxed(),
        })
    }

    /// Given a `Read`, create a `Read` which data are decompressed with `FileCompressionType`.
    pub fn convert_read<T: std::io::Read + Send + 'static>(
        &self,
        r: T,
    ) -> Result<Box<dyn std::io::Read + Send>> {
        Ok(match self.variant {
            #[cfg(feature = "compression")]
            GZIP => Box::new(MultiGzDecoder::new(r)),
            #[cfg(feature = "compression")]
            BZIP2 => Box::new(MultiBzDecoder::new(r)),
            #[cfg(feature = "compression")]
            XZ => Box::new(XzDecoder::new_multi_decoder(r)),
            #[cfg(feature = "compression")]
            ZSTD => match ZstdDecoder::new(r) {
                Ok(decoder) => Box::new(decoder),
                Err(e) => return Err(DataFusionError::External(Box::new(e))),
            },
            #[cfg(not(feature = "compression"))]
            GZIP | BZIP2 | XZ | ZSTD => {
                return Err(DataFusionError::NotImplemented(
                    "Compression feature is not enabled".to_owned(),
                ))
            }
            UNCOMPRESSED => Box::new(r),
        })
    }
}

/// Trait for extending the functionality of the `FileType` enum.
pub trait FileTypeExt {
    /// Given a `FileCompressionType`, return the `FileType`'s extension with compression suffix
    fn get_ext_with_compression(&self, c: FileCompressionType) -> Result<String>;
}

impl FileTypeExt for FileType {
    fn get_ext_with_compression(&self, c: FileCompressionType) -> Result<String> {
        let ext = self.get_ext();

        match self {
            FileType::JSON | FileType::CSV => Ok(format!("{}{}", ext, c.get_ext())),
            FileType::AVRO | FileType::ARROW => match c.variant {
                UNCOMPRESSED => Ok(ext),
                _ => Err(DataFusionError::Internal(
                    "FileCompressionType can be specified for CSV/JSON FileType.".into(),
                )),
            },
            FileType::PARQUET => match c.variant {
                UNCOMPRESSED => Ok(ext),
                _ => Err(DataFusionError::Internal(
                    "FileCompressionType can be specified for CSV/JSON FileType.".into(),
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::file_format::file_compression_type::{
        FileCompressionType, FileTypeExt,
    };
    use crate::error::DataFusionError;
    use datafusion_common::file_options::file_type::FileType;
    use std::str::FromStr;

    #[test]
    fn get_ext_with_compression() {
        for (file_type, compression, extension) in [
            (FileType::CSV, FileCompressionType::UNCOMPRESSED, ".csv"),
            (FileType::CSV, FileCompressionType::GZIP, ".csv.gz"),
            (FileType::CSV, FileCompressionType::XZ, ".csv.xz"),
            (FileType::CSV, FileCompressionType::BZIP2, ".csv.bz2"),
            (FileType::CSV, FileCompressionType::ZSTD, ".csv.zst"),
            (FileType::JSON, FileCompressionType::UNCOMPRESSED, ".json"),
            (FileType::JSON, FileCompressionType::GZIP, ".json.gz"),
            (FileType::JSON, FileCompressionType::XZ, ".json.xz"),
            (FileType::JSON, FileCompressionType::BZIP2, ".json.bz2"),
            (FileType::JSON, FileCompressionType::ZSTD, ".json.zst"),
        ] {
            assert_eq!(
                file_type.get_ext_with_compression(compression).unwrap(),
                extension
            );
        }

        let mut ty_ext_tuple = vec![];
        ty_ext_tuple.push((FileType::AVRO, ".avro"));
        ty_ext_tuple.push((FileType::PARQUET, ".parquet"));

        // Cannot specify compression for these file types
        for (file_type, extension) in ty_ext_tuple {
            assert_eq!(
                file_type
                    .get_ext_with_compression(FileCompressionType::UNCOMPRESSED)
                    .unwrap(),
                extension
            );
            for compression in [
                FileCompressionType::GZIP,
                FileCompressionType::XZ,
                FileCompressionType::BZIP2,
                FileCompressionType::ZSTD,
            ] {
                assert!(matches!(
                    file_type.get_ext_with_compression(compression),
                    Err(DataFusionError::Internal(_))
                ));
            }
        }
    }

    #[test]
    fn from_str() {
        for (ext, compression_type) in [
            ("gz", FileCompressionType::GZIP),
            ("GZ", FileCompressionType::GZIP),
            ("gzip", FileCompressionType::GZIP),
            ("GZIP", FileCompressionType::GZIP),
            ("xz", FileCompressionType::XZ),
            ("XZ", FileCompressionType::XZ),
            ("bz2", FileCompressionType::BZIP2),
            ("BZ2", FileCompressionType::BZIP2),
            ("bzip2", FileCompressionType::BZIP2),
            ("BZIP2", FileCompressionType::BZIP2),
            ("zst", FileCompressionType::ZSTD),
            ("ZST", FileCompressionType::ZSTD),
            ("zstd", FileCompressionType::ZSTD),
            ("ZSTD", FileCompressionType::ZSTD),
            ("", FileCompressionType::UNCOMPRESSED),
        ] {
            assert_eq!(
                FileCompressionType::from_str(ext).unwrap(),
                compression_type
            );
        }

        assert!(matches!(
            FileCompressionType::from_str("Unknown"),
            Err(DataFusionError::NotImplemented(_))
        ));
    }
}
