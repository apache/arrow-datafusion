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

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetResult, ListResult, ObjectMeta, ObjectStore};
use object_store::{MultipartId, Result};
use std::fmt::{Debug, Display, Formatter};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::AsyncWrite;

/// Wraps a [`ObjectStore`] and makes its get response return chunks
///
/// TODO: Upstream into object_store_rs
#[derive(Debug)]
pub struct ChunkedStore {
    inner: Arc<dyn ObjectStore>,
    chunk_size: usize,
}

impl ChunkedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, chunk_size: usize) -> Self {
        Self { inner, chunk_size }
    }
}

impl Display for ChunkedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChunkedStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ChunkedStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.inner.put(location, bytes).await
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        self.inner.put_multipart(location).await
    }

    async fn abort_multipart(
        &self,
        location: &Path,
        multipart_id: &MultipartId,
    ) -> Result<()> {
        self.inner.abort_multipart(location, multipart_id).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let bytes = self.inner.get(location).await?.bytes().await?;
        let mut offset = 0;
        let chunk_size = self.chunk_size;

        Ok(GetResult::Stream(
            futures::stream::iter(std::iter::from_fn(move || {
                let remaining = bytes.len() - offset;
                if remaining == 0 {
                    return None;
                }
                let to_read = remaining.min(chunk_size);
                let next_offset = offset + to_read;
                let slice = bytes.slice(offset..next_offset);
                offset = next_offset;
                Some(Ok(slice))
            }))
            .boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        self.inner.get_range(location, range).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.inner.delete(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        self.inner.list(prefix).await
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_chunked() {
        let location = Path::parse("test").unwrap();
        let store = Arc::new(InMemory::new());
        store
            .put(&location, Bytes::from(vec![0; 1001]))
            .await
            .unwrap();

        for chunk_size in [10, 20, 31] {
            let store = ChunkedStore::new(store.clone(), chunk_size);
            let mut s = match store.get(&location).await.unwrap() {
                GetResult::Stream(s) => s,
                _ => unreachable!(),
            };

            let mut remaining = 1001;
            while let Some(next) = s.next().await {
                let size = next.unwrap().len();
                let expected = remaining.min(chunk_size);
                assert_eq!(size, expected);
                remaining -= expected;
            }
            assert_eq!(remaining, 0);
        }
    }
}
