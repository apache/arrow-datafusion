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

pub mod object_store;

use chrono::{DateTime, Utc};
use datafusion_common::ScalarValue;

/// Represents a specific file or a prefix (folder) that may
/// require further resolution
#[derive(Debug)]
pub enum ListEntry {
    /// Specific file with metadata
    FileMeta(FileMeta),
    /// Prefix to be further resolved during partition discovery
    Prefix(String),
}

/// The path and size of the file.
#[derive(Debug, Clone, PartialEq)]
pub struct SizedFile {
    /// Path of the file. It is relative to the current object
    /// store (it does not specify the `xx://` scheme).
    pub path: String,
    /// File size in total
    pub size: u64,
}

/// Description of a file as returned by the listing command of a
/// given object store. The resulting path is relative to the
/// object store that generated it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMeta {
    /// The path and size of the file.
    pub sized_file: SizedFile,
    /// The last modification time of the file according to the
    /// object store metadata. This information might be used by
    /// catalog systems like Delta Lake for time travel (see
    /// <https://github.com/delta-io/delta/issues/192>)
    pub last_modified: Option<DateTime<Utc>>,
}

impl FileMeta {
    /// The path that describes this file. It is relative to the
    /// associated object store.
    pub fn path(&self) -> &str {
        &self.sized_file.path
    }

    /// The size of the file.
    pub fn size(&self) -> u64 {
        self.sized_file.size
    }
}

impl std::fmt::Display for FileMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} (size: {})", self.path(), self.size())
    }
}

#[derive(Debug, Clone)]
/// A single file that should be read, along with its schema, statistics
/// and partition column values that need to be appended to each row.
pub struct PartitionedFile {
    /// Path for the file (e.g. URL, filesystem path, etc)
    pub file_meta: FileMeta,
    /// Values of partition columns to be appended to each row
    pub partition_values: Vec<ScalarValue>,
    // We may include row group range here for a more fine-grained parallel execution
}

impl PartitionedFile {
    /// Create a simple file without metadata or partition
    pub fn new(path: String, size: u64) -> Self {
        Self {
            file_meta: FileMeta {
                sized_file: SizedFile { path, size },
                last_modified: None,
            },
            partition_values: vec![],
        }
    }
}

impl std::fmt::Display for PartitionedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.file_meta)
    }
}
