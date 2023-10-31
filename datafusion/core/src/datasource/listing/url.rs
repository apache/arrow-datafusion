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

use std::path::{Component, PathBuf};

use crate::datasource::object_store::ObjectStoreUrl;
use crate::execution::context::SessionState;
use datafusion_common::{DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use log::debug;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use percent_encoding;
use std::sync::Arc;
use url::Url;

/// A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]
/// for more information on the supported expressions
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ListingTableUrl {
    /// A URL that identifies a file or directory to list files from
    url: Url,
    /// The path prefix
    prefix: Path,
    /// An optional glob expression used to filter files
    glob: Option<Pattern>,
}

impl ListingTableUrl {
    /// Parse a provided string as a `ListingTableUrl`
    ///
    /// A URL can either refer to a single object, or a collection of objects with a
    /// common prefix, with the presence of a trailing `/` indicating a collection.
    ///
    /// For example, `file:///foo.txt` refers to the file at `/foo.txt`, whereas
    /// `file:///foo/` refers to all the files under the directory `/foo` and its
    /// subdirectories.
    ///
    /// Similarly `s3://BUCKET/blob.csv` refers to `blob.csv` in the S3 bucket `BUCKET`,
    /// wherease `s3://BUCKET/foo/` refers to all objects with the prefix `foo/` in the
    /// S3 bucket `BUCKET`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined by [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// If the path contains any of `'?', '*', '['`, it will be considered
    /// a glob expression and resolved as described in the section below.
    ///
    /// Otherwise, the path will be resolved to an absolute path based on the current
    /// working directory, and converted to a [file URI].
    ///
    /// If the path already exists in the local filesystem this will be used to determine if this
    /// [`ListingTableUrl`] refers to a collection or a single object, otherwise the presence
    /// of a trailing path delimiter will be used to indicate a directory. For the avoidance
    /// of ambiguity it is recommended users always include trailing `/` when intending to
    /// refer to a directory.
    ///
    /// ## Glob File Paths
    ///
    /// If no scheme is provided, and the path contains a glob expression, it will
    /// be resolved as follows.
    ///
    /// The string up to the first path segment containing a glob expression will be extracted,
    /// and resolved in the same manner as a normal scheme-less path above.
    ///
    /// The remaining string will be interpreted as a [`glob::Pattern`] and used as a
    /// filter when listing files from object storage
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    /// [URL]: https://url.spec.whatwg.org/
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self::new(url, None)),
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::parse_path(s),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }
    }

    /// Creates a new [`ListingTableUrl`] interpreting `s` as a filesystem path
    fn parse_path(s: &str) -> Result<Self> {
        let (prefix, glob) = match split_glob_expression(s) {
            Some((prefix, glob)) => {
                let glob = Pattern::new(glob)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                (prefix, Some(glob))
            }
            None => (s, None),
        };

        let url = url_from_path(prefix).ok_or_else(|| {
            DataFusionError::Internal(format!("Can not open path: {s}"))
        })?;
        Ok(Self::new(url, glob))
    }

    /// Creates a new [`ListingTableUrl`] from a url and optional glob expression
    fn new(url: Url, glob: Option<Pattern>) -> Self {
        let decoded_path =
            percent_encoding::percent_decode_str(url.path()).decode_utf8_lossy();
        let prefix = Path::from(decoded_path.as_ref());
        Self { url, prefix, glob }
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Return the prefix from which to list files
    pub fn prefix(&self) -> &Path {
        &self.prefix
    }

    /// Returns `true` if `path` matches this [`ListingTableUrl`]
    pub fn contains(&self, path: &Path) -> bool {
        match self.strip_prefix(path) {
            Some(mut segments) => match &self.glob {
                Some(glob) => {
                    let stripped = segments.join("/");
                    glob.matches(&stripped)
                }
                None => true,
            },
            None => false,
        }
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    pub(crate) fn strip_prefix<'a, 'b: 'a>(
        &'a self,
        path: &'b Path,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        use object_store::path::DELIMITER;
        let mut stripped = path.as_ref().strip_prefix(self.prefix.as_ref())?;
        if !stripped.is_empty() && !self.prefix.as_ref().is_empty() {
            stripped = stripped.strip_prefix(DELIMITER)?;
        }
        Some(stripped.split_terminator(DELIMITER))
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub(crate) async fn list_all_files<'a>(
        &'a self,
        ctx: &'a SessionState,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
        // If the prefix is a file, use a head request, otherwise list
        let is_dir = self.url.as_str().ends_with('/');
        let list = match is_dir {
            true => match ctx.runtime_env().cache_manager.get_list_files_cache() {
                None => futures::stream::once(store.list(Some(&self.prefix)))
                    .try_flatten()
                    .boxed(),
                Some(cache) => {
                    if let Some(res) = cache.get(&self.prefix) {
                        debug!("Hit list all files cache");
                        futures::stream::iter(res.as_ref().clone().into_iter().map(Ok))
                            .boxed()
                    } else {
                        let list_res = store.list(Some(&self.prefix)).await;
                        let vec = list_res?.try_collect::<Vec<ObjectMeta>>().await?;
                        cache.put(&self.prefix, Arc::new(vec.clone()));
                        futures::stream::iter(vec.into_iter().map(Ok)).boxed()
                    }
                }
            },
            false => futures::stream::once(store.head(&self.prefix))
                .filter(|r| {
                    let p = !matches!(r, Err(object_store::Error::NotFound { .. }));
                    futures::future::ready(p)
                })
                .boxed(),
        };
        Ok(list
            .try_filter(move |meta| {
                let path = &meta.location;
                let extension_match = path.as_ref().ends_with(file_extension);
                let glob_match = self.contains(path);
                futures::future::ready(extension_match && glob_match)
            })
            .map_err(DataFusionError::ObjectStore)
            .boxed())
    }

    /// Returns this [`ListingTableUrl`] as a string
    pub fn as_str(&self) -> &str {
        self.as_ref()
    }

    /// Return the [`ObjectStoreUrl`] for this [`ListingTableUrl`]
    pub fn object_store(&self) -> ObjectStoreUrl {
        let url = &self.url[url::Position::BeforeScheme..url::Position::BeforePath];
        ObjectStoreUrl::parse(url).unwrap()
    }
}

impl AsRef<str> for ListingTableUrl {
    fn as_ref(&self) -> &str {
        self.url.as_ref()
    }
}

impl AsRef<Url> for ListingTableUrl {
    fn as_ref(&self) -> &Url {
        &self.url
    }
}

impl std::fmt::Display for ListingTableUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_str().fmt(f)
    }
}

fn url_from_path(s: &str) -> Option<Url> {
    let path = std::path::Path::new(s);
    let is_dir = match path.exists() {
        true => path.is_dir(),
        // Fallback to inferring from trailing separator
        false => std::path::is_separator(s.chars().last()?),
    };

    let p = match path.is_absolute() {
        true => resolve_path(path)?,
        false => {
            let absolute = std::env::current_dir().ok()?.join(path);
            resolve_path(&absolute)?
        }
    };

    match is_dir {
        true => Url::from_directory_path(p).ok(),
        false => Url::from_file_path(p).ok(),
    }
}

fn resolve_path(path: &std::path::Path) -> Option<PathBuf> {
    let mut base = PathBuf::with_capacity(path.as_os_str().len());
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => base.push(component.as_os_str()),
            Component::Normal(p) => base.push(p),
            Component::CurDir => {} // Do nothing
            Component::ParentDir => {
                if !base.pop() {
                    return None;
                }
            }
        }
    }
    Some(base)
}

const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

/// Splits `path` at the first path segment containing a glob expression, returning
/// `None` if no glob expression found.
///
/// Path delimiters are determined using [`std::path::is_separator`] which
/// permits `/` as a path delimiter even on Windows platforms.
///
fn split_glob_expression(path: &str) -> Option<(&str, &str)> {
    let mut last_separator = 0;

    for (byte_idx, char) in path.char_indices() {
        if GLOB_START_CHARS.contains(&char) {
            if last_separator == 0 {
                return Some((".", path));
            }
            return Some(path.split_at(last_separator));
        }

        if std::path::is_separator(char) {
            last_separator = byte_idx + char.len_utf8();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_path() {
        let root = std::env::current_dir().unwrap();
        let root = root.to_string_lossy();

        let url = ListingTableUrl::parse(root).unwrap();
        let child = url.prefix.child("partition").child("file");

        let prefix: Vec<_> = url.strip_prefix(&child).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "file"]);

        let url = ListingTableUrl::parse("file:///").unwrap();
        let child = Path::parse("/foo/bar").unwrap();
        let prefix: Vec<_> = url.strip_prefix(&child).unwrap().collect();
        assert_eq!(prefix, vec!["foo", "bar"]);

        let url = ListingTableUrl::parse("file:///foo").unwrap();
        let child = Path::parse("/foob/bar").unwrap();
        assert!(url.strip_prefix(&child).is_none());

        let url = ListingTableUrl::parse("file:///foo/file").unwrap();
        let child = Path::parse("/foo/file").unwrap();
        assert_eq!(url.strip_prefix(&child).unwrap().count(), 0);

        let url = ListingTableUrl::parse("file:///foo/ bar").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/ bar");

        let url = ListingTableUrl::parse("file:///foo/bar?").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar");

        let url = ListingTableUrl::parse("file:///foo/😺").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/%F0%9F%98%BA");
    }

    #[test]
    fn test_prefix_s3() {
        let url = ListingTableUrl::parse("s3://bucket/foo/bar").unwrap();
        assert_eq!(url.prefix.as_ref(), "foo/bar");

        let path = Path::from("foo/bar/partition/foo.parquet");
        let prefix: Vec<_> = url.strip_prefix(&path).unwrap().collect();
        assert_eq!(prefix, vec!["partition", "foo.parquet"]);

        let path = Path::from("other/bar/partition/foo.parquet");
        assert!(url.strip_prefix(&path).is_none());
    }

    #[test]
    fn test_split_glob() {
        fn test(input: &str, expected: Option<(&str, &str)>) {
            assert_eq!(
                split_glob_expression(input),
                expected,
                "testing split_glob_expression with {input}"
            );
        }

        // no glob patterns
        test("/", None);
        test("/a.txt", None);
        test("/a", None);
        test("/a/", None);
        test("/a/b", None);
        test("/a/b/", None);
        test("/a/b.txt", None);
        test("/a/b/c.txt", None);
        // glob patterns, thus we build the longest path (os-specific)
        test("*.txt", Some((".", "*.txt")));
        test("/*.txt", Some(("/", "*.txt")));
        test("/a/*b.txt", Some(("/a/", "*b.txt")));
        test("/a/*/b.txt", Some(("/a/", "*/b.txt")));
        test("/a/b/[123]/file*.txt", Some(("/a/b/", "[123]/file*.txt")));
        test("/a/b*.txt", Some(("/a/", "b*.txt")));
        test("/a/b/**/c*.txt", Some(("/a/b/", "**/c*.txt")));

        // https://github.com/apache/arrow-datafusion/issues/2465
        test(
            "/a/b/c//alltypes_plain*.parquet",
            Some(("/a/b/c//", "alltypes_plain*.parquet")),
        );
    }

    #[test]
    fn test_resolve_path() {
        let r = resolve_path("/foo/bar/../baz.txt".as_ref()).unwrap();
        assert_eq!(r.to_str().unwrap(), "/foo/baz.txt");

        let r = resolve_path("/foo/bar/./baz.txt".as_ref()).unwrap();
        assert_eq!(r.to_str().unwrap(), "/foo/bar/baz.txt");

        let r = resolve_path("/foo/bar/../../../baz.txt".as_ref());
        assert_eq!(r, None);
    }

    #[test]
    fn test_url_from_path() {
        let url = url_from_path("foo/bar").unwrap();
        assert!(url.path().ends_with("foo/bar"));

        let url = url_from_path("foo/bar/").unwrap();
        assert!(url.path().ends_with("foo/bar/"));
    }
}
