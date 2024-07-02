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

//! Module containing helper methods for the various file formats
//! See write.rs for write related helper methods

/// Default max records to scan to infer the schema
pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

pub mod arrow;
pub mod avro;
pub mod csv;
pub mod file_compression_type;
pub mod json;
pub mod options;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod write;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::sync::Arc;

use crate::arrow::datatypes::SchemaRef;
use crate::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::{ExecutionPlan, Statistics};

use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{internal_err, not_impl_err, GetExt};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortRequirement};

use async_trait::async_trait;
use file_compression_type::FileCompressionType;
use object_store::{ObjectMeta, ObjectStore};

/// Factory for creating [`FileFormat`] instances based on session and command level options
///
/// Users can provide their own `FileFormatFactory` to support arbitrary file formats
pub trait FileFormatFactory: Sync + Send + GetExt {
    /// Initialize a [FileFormat] and configure based on session and command level options
    fn create(
        &self,
        state: &SessionState,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>>;

    /// Initialize a [FileFormat] with all options set to default values
    fn default(&self) -> Arc<dyn FileFormat>;
}

/// This trait abstracts all the file format specific implementations
/// from the [`TableProvider`]. This helps code re-utilization across
/// providers that support the same file formats.
///
/// [`TableProvider`]: crate::datasource::provider::TableProvider
#[async_trait]
pub trait FileFormat: Send + Sync + fmt::Debug {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String;

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String>;

    /// Infer the common schema of the provided objects. The objects will usually
    /// be analysed up to a given number of records or files (as specified in the
    /// format config) then give the estimated common schema. This might fail if
    /// the files have schemas that cannot be merged.
    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef>;

    /// Infer the statistics for the provided object. The cost and accuracy of the
    /// estimated statistics might vary greatly between file formats.
    ///
    /// `table_schema` is the (combined) schema of the overall table
    /// and may be a superset of the schema contained in this file.
    ///
    /// TODO: should the file source return statistics for only columns referred to in the table schema?
    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics>;

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Take a list of files and the configuration to convert it to the
    /// appropriate writer executor according to this file format.
    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        _conf: FileSinkConfig,
        _order_requirements: Option<Vec<PhysicalSortRequirement>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("Writer not implemented for this format")
    }
}

/// A container of [FileFormatFactory] which also implements [FileType].
/// This enables converting a dyn FileFormat to a dyn FileType.
/// The former trait is a superset of the latter trait, which includes execution time
/// relevant methods. [FileType] is only used in logical planning and only implements
/// the subset of methods required during logical planning.
pub struct DefaultFileType {
    file_format_factory: Arc<dyn FileFormatFactory>,
}

impl DefaultFileType {
    /// Constructs a [DefaultFileType] wrapper from a [FileFormatFactory]
    pub fn new(file_format_factory: Arc<dyn FileFormatFactory>) -> Self {
        Self {
            file_format_factory,
        }
    }
}

impl FileType for DefaultFileType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for DefaultFileType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.file_format_factory.default().fmt(f)
    }
}

impl GetExt for DefaultFileType {
    fn get_ext(&self) -> String {
        self.file_format_factory.get_ext()
    }
}

/// Converts a [FileFormatFactory] to a [FileType]
pub fn format_as_file_type(
    file_format_factory: Arc<dyn FileFormatFactory>,
) -> Arc<dyn FileType> {
    Arc::new(DefaultFileType {
        file_format_factory,
    })
}

/// Converts a [FileType] to a [FileFormatFactory].
/// Returns an error if the [FileType] cannot be
/// downcasted to a [DefaultFileType].
pub fn file_type_to_format(
    file_type: &Arc<dyn FileType>,
) -> datafusion_common::Result<Arc<dyn FileFormatFactory>> {
    match file_type
        .as_ref()
        .as_any()
        .downcast_ref::<DefaultFileType>()
    {
        Some(source) => Ok(Arc::clone(&source.file_format_factory)),
        _ => internal_err!("FileType was not DefaultFileType"),
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::ops::Range;
    use std::sync::Mutex;

    use super::*;
    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::test::object_store::local_unpartitioned_file;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use object_store::{
        Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
        PutMultipartOpts, PutOptions, PutPayload, PutResult,
    };

    pub async fn scan_format(
        state: &SessionState,
        format: &dyn FileFormat,
        store_root: &str,
        file_name: &str,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let store = Arc::new(LocalFileSystem::new()) as _;
        let meta = local_unpartitioned_file(format!("{store_root}/{file_name}"));

        let file_schema = format.infer_schema(state, &store, &[meta.clone()]).await?;

        let statistics = format
            .infer_stats(state, &store, file_schema.clone(), &meta)
            .await?;

        let file_groups = vec![vec![PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
        }]];

        let exec = format
            .create_physical_plan(
                state,
                FileScanConfig::new(ObjectStoreUrl::local_filesystem(), file_schema)
                    .with_file_groups(file_groups)
                    .with_statistics(statistics)
                    .with_projection(projection)
                    .with_limit(limit),
                None,
            )
            .await?;
        Ok(exec)
    }

    /// Mock ObjectStore to provide an variable stream of bytes on get
    /// Able to keep track of how many iterations of the provided bytes were repeated
    #[derive(Debug)]
    pub struct VariableStream {
        bytes_to_repeat: Bytes,
        max_iterations: usize,
        iterations_detected: Arc<Mutex<usize>>,
    }

    impl std::fmt::Display for VariableStream {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "VariableStream")
        }
    }

    #[async_trait]
    impl ObjectStore for VariableStream {
        async fn put_opts(
            &self,
            _location: &Path,
            _payload: PutPayload,
            _opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _location: &Path,
            _opts: PutMultipartOpts,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            unimplemented!()
        }

        async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
            let bytes = self.bytes_to_repeat.clone();
            let range = 0..bytes.len() * self.max_iterations;
            let arc = self.iterations_detected.clone();
            let stream = futures::stream::repeat_with(move || {
                let arc_inner = arc.clone();
                *arc_inner.lock().unwrap() += 1;
                Ok(bytes.clone())
            })
            .take(self.max_iterations)
            .boxed();

            Ok(GetResult {
                payload: GetResultPayload::Stream(stream),
                meta: ObjectMeta {
                    location: location.clone(),
                    last_modified: Default::default(),
                    size: range.end,
                    e_tag: None,
                    version: None,
                },
                range: Default::default(),
                attributes: Attributes::default(),
            })
        }

        async fn get_opts(
            &self,
            _location: &Path,
            _opts: GetOptions,
        ) -> object_store::Result<GetResult> {
            unimplemented!()
        }

        async fn get_ranges(
            &self,
            _location: &Path,
            _ranges: &[Range<usize>],
        ) -> object_store::Result<Vec<Bytes>> {
            unimplemented!()
        }

        async fn head(&self, _location: &Path) -> object_store::Result<ObjectMeta> {
            unimplemented!()
        }

        async fn delete(&self, _location: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        fn list(
            &self,
            _prefix: Option<&Path>,
        ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
            unimplemented!()
        }

        async fn list_with_delimiter(
            &self,
            _prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            unimplemented!()
        }

        async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
            unimplemented!()
        }

        async fn copy_if_not_exists(
            &self,
            _from: &Path,
            _to: &Path,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    impl VariableStream {
        pub fn new(bytes_to_repeat: Bytes, max_iterations: usize) -> Self {
            Self {
                bytes_to_repeat,
                max_iterations,
                iterations_detected: Arc::new(Mutex::new(0)),
            }
        }

        pub fn get_iterations_detected(&self) -> usize {
            *self.iterations_detected.lock().unwrap()
        }
    }
}
