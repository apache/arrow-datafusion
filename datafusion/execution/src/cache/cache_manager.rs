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

use crate::cache::CacheAccessor;
use datafusion_common::{Result, Statistics};
use object_store::path::Path;
use object_store::ObjectMeta;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// The cache of listing files statistics.
/// if set [`CacheManagerConfig::with_files_statistics_cache`]
/// Will avoid infer same file statistics repeatedly during the session lifetime,
/// this cache will store in [`crate::runtime_env::RuntimeEnv`].
pub type FileStatisticsCache =
    Arc<dyn CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta>>;

pub type ListFilesCache =
    Arc<dyn CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = ObjectMeta>>;

impl Debug for dyn CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

impl Debug for dyn CacheAccessor<Path, Arc<Vec<ObjectMeta>>, Extra = ObjectMeta> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache name: {} with length: {}", self.name(), self.len())
    }
}

#[derive(Default, Debug)]
pub struct CacheManager {
    file_statistic_cache: Option<FileStatisticsCache>,
    list_files_cache: Option<ListFilesCache>,
}

impl CacheManager {
    pub fn try_new(config: &CacheManagerConfig) -> Result<Arc<Self>> {
        let mut manager = CacheManager::default();
        if let Some(cc) = &config.table_files_statistics_cache {
            manager.file_statistic_cache = Some(Arc::clone(cc))
        }
        if let Some(lc) = &config.list_files_cache {
            manager.list_files_cache = Some(Arc::clone(lc))
        }
        Ok(Arc::new(manager))
    }

    /// Get the cache of listing files statistics.
    pub fn get_file_statistic_cache(&self) -> Option<FileStatisticsCache> {
        self.file_statistic_cache.clone()
    }

    /// Get the cache of objectMeta under same path.
    pub fn get_list_files_cache(&self) -> Option<ListFilesCache> {
        self.list_files_cache.clone()
    }
}

#[derive(Clone, Default)]
pub struct CacheManagerConfig {
    /// Enable cache of files statistics when listing files.
    /// Avoid get same file statistics repeatedly in same datafusion session.
    /// Default is disable. Fow now only supports Parquet files.
    pub table_files_statistics_cache: Option<FileStatisticsCache>,
    /// Enable cache of file metadata when listing files.
    /// This setting avoids listing file meta of the same path repeatedly
    /// in same session, which may be expensive in certain situations (e.g. remote object storage).
    /// Note that if this option is enabled, DataFusion will not see any updates to the underlying
    /// location.  
    /// Default is disable.
    pub list_files_cache: Option<ListFilesCache>,
}

impl CacheManagerConfig {
    pub fn with_files_statistics_cache(
        mut self,
        cache: Option<FileStatisticsCache>,
    ) -> Self {
        self.table_files_statistics_cache = cache;
        self
    }

    pub fn with_list_files_cache(mut self, cache: Option<ListFilesCache>) -> Self {
        self.list_files_cache = cache;
        self
    }
}
