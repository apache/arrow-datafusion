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

//! Object store that represents the Local File System.

use std::fs::{self, File, Metadata};
use std::io::{BufReader, Seek, SeekFrom};
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, AsyncRead, StreamExt};

use crate::datasource::object_store::{
    FileMeta, FileMetaStream, ListEntryStream, ObjectReader, ObjectStore,
};
use crate::datasource::PartitionedFile;
use crate::error::{DataFusionError, Result};

use super::{ObjectReaderStream, ReadSeek, SizedFile};

#[derive(Debug)]
/// Local File System as Object Store.
pub struct LocalFileSystem;

#[async_trait]
impl ObjectStore for LocalFileSystem {
    async fn list_file(&self, prefix: &str) -> Result<FileMetaStream> {
        let prefix = if let Some((_scheme, path)) = prefix.split_once("://") {
            path
        } else {
            prefix
        };
        list_all(prefix.to_owned()).await
    }

    async fn list_dir(
        &self,
        _prefix: &str,
        _delimiter: Option<String>,
    ) -> Result<ListEntryStream> {
        todo!()
    }

    fn file_reader(&self, file: SizedFile) -> Result<Arc<dyn ObjectReader>> {
        Ok(Arc::new(LocalFileReader::new(file)?))
    }
}

struct LocalFileReader {
    file: SizedFile,
}

impl LocalFileReader {
    fn new(file: SizedFile) -> Result<Self> {
        Ok(Self { file })
    }
}

#[async_trait]
impl ObjectReader for LocalFileReader {
    async fn chunk_reader(
        &self,
        _start: u64,
        _length: usize,
    ) -> Result<Box<dyn AsyncRead>> {
        todo!(
            "implement once async file readers are available (arrow-rs#78, arrow-rs#111)"
        )
    }

    fn sync_chunk_reader(
        &self,
        start: u64,
        _length: usize,
    ) -> Result<Box<dyn ReadSeek + Send + Sync>> {
        // A new file descriptor is opened for each chunk reader.
        // This okay because chunks are usually fairly large.
        let mut file = File::open(&self.file.path)?;
        file.seek(SeekFrom::Start(start))?;

        let file = BufReader::new(file);
        Ok(Box::new(file))
    }

    fn length(&self) -> u64 {
        self.file.size
    }
}

async fn list_all(prefix: String) -> Result<FileMetaStream> {
    fn get_meta(path: String, metadata: Metadata) -> FileMeta {
        FileMeta {
            sized_file: SizedFile {
                path,
                size: metadata.len(),
            },
            last_modified: metadata.modified().map(chrono::DateTime::from).ok(),
        }
    }

    async fn find_files_in_dir(
        path: String,
        to_visit: &mut Vec<String>,
    ) -> Result<Vec<FileMeta>> {
        let mut dir = tokio::fs::read_dir(path).await?;
        let mut files = Vec::new();

        while let Some(child) = dir.next_entry().await? {
            if let Some(child_path) = child.path().to_str() {
                let metadata = child.metadata().await?;
                if metadata.is_dir() {
                    to_visit.push(child_path.to_string());
                } else {
                    files.push(get_meta(child_path.to_owned(), metadata))
                }
            } else {
                return Err(DataFusionError::Plan("Invalid path".to_string()));
            }
        }
        Ok(files)
    }

    let prefix_meta = tokio::fs::metadata(&prefix).await?;
    let prefix = prefix.to_owned();
    if prefix_meta.is_file() {
        Ok(Box::pin(stream::once(async move {
            Ok(get_meta(prefix, prefix_meta))
        })))
    } else {
        let result = stream::unfold(vec![prefix], move |mut to_visit| async move {
            match to_visit.pop() {
                None => None,
                Some(path) => {
                    let file_stream = match find_files_in_dir(path, &mut to_visit).await {
                        Ok(files) => stream::iter(files).map(Ok).left_stream(),
                        Err(e) => stream::once(async { Err(e) }).right_stream(),
                    };

                    Some((file_stream, to_visit))
                }
            }
        })
        .flatten();
        Ok(Box::pin(result))
    }
}

/// Create a stream of `ObjectReader` by converting each file in the `files` vector
/// into instances of `LocalFileReader`
pub fn local_object_reader_stream(files: Vec<String>) -> ObjectReaderStream {
    Box::pin(futures::stream::iter(files).map(|f| Ok(local_object_reader(f))))
}

/// Helper method to convert a file location to a `LocalFileReader`
pub fn local_object_reader(file: String) -> Arc<dyn ObjectReader> {
    LocalFileSystem
        .file_reader(local_unpartitioned_file(file).file_meta.sized_file)
        .expect("File not found")
}

/// Helper method to fetch the file size and date at given path and create a `FileMeta`
pub fn local_unpartitioned_file(file: String) -> PartitionedFile {
    let metadata = fs::metadata(&file).expect("Local file metadata");
    PartitionedFile {
        file_meta: FileMeta {
            sized_file: SizedFile {
                size: metadata.len(),
                path: file,
            },
            last_modified: metadata.modified().map(chrono::DateTime::from).ok(),
        },
        partition_values: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::collections::HashSet;
    use std::fs::create_dir;
    use std::fs::File;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_recursive_listing() -> Result<()> {
        // tmp/a.txt
        // tmp/x/b.txt
        // tmp/y/c.txt
        let tmp = tempdir()?;
        let x_path = tmp.path().join("x");
        let y_path = tmp.path().join("y");
        let a_path = tmp.path().join("a.txt");
        let b_path = x_path.join("b.txt");
        let c_path = y_path.join("c.txt");
        create_dir(&x_path)?;
        create_dir(&y_path)?;
        File::create(&a_path)?;
        File::create(&b_path)?;
        File::create(&c_path)?;

        let mut all_files = HashSet::new();
        let mut files = list_all(tmp.path().to_str().unwrap().to_string()).await?;
        while let Some(file) = files.next().await {
            let file = file?;
            assert_eq!(file.size(), 0);
            all_files.insert(file.path().to_owned());
        }

        assert_eq!(all_files.len(), 3);
        assert!(all_files.contains(a_path.to_str().unwrap()));
        assert!(all_files.contains(b_path.to_str().unwrap()));
        assert!(all_files.contains(c_path.to_str().unwrap()));

        Ok(())
    }
}
