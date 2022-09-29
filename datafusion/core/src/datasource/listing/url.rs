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

use crate::datasource::object_store::ObjectStoreUrl;
use datafusion_common::{DataFusionError, Result};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use glob::Pattern;
use itertools::Itertools;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use url::Url;

/// A parsed URL identifying files for a listing table, see [`ListingTableUrl::parse`]
/// for more information on the supported expressions
#[derive(Clone)]
pub struct ListingTableUrl {
    /// A URL that identifies a file or directory to list files from
    url: Url,
    /// The path prefix
    prefix: Path,
    /// An optional predicate used to filter files
    predicate: Option<Arc<Box<dyn Fn(&ObjectMeta) -> bool + Sync + Send>>>,
}

impl ListingTableUrl {
    /// Parse a provided string as a `ListingTableUrl`
    ///
    /// # Paths without a Scheme
    ///
    /// If no scheme is provided, or the string is an absolute filesystem path
    /// as determined [`std::path::Path::is_absolute`], the string will be
    /// interpreted as a path on the local filesystem using the operating
    /// system's standard path delimiter, i.e. `\` on Windows, `/` on Unix.
    ///
    /// If the path contains any of `'?', '*', '['`, it will be considered
    /// a glob expression and resolved as described in the section below.
    ///
    /// Otherwise, the path will be resolved to an absolute path, returning
    /// an error if it does not exist, and converted to a [file URI]
    ///
    /// If you wish to specify a path that does not exist on the local
    /// machine you must provide it as a fully-qualified [file URI]
    /// e.g. `file:///myfile.txt`
    ///
    /// ## Glob File Paths
    ///
    /// If no scheme is provided, and the path contains a glob expression, it will
    /// be resolved as follows.
    ///
    /// The string up to the first path segment containing a glob expression will be extracted,
    /// and resolved in the same manner as a normal scheme-less path. That is, resolved to
    /// an absolute path on the local filesystem, returning an error if it does not exist,
    /// and converted to a [file URI]
    ///
    /// The remaining string will be interpreted as a [`glob::Pattern`] and used as a
    /// filter when listing files from object storage
    ///
    /// [file URI]: https://en.wikipedia.org/wiki/File_URI_scheme
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        let s = s.as_ref();

        // This is necessary to handle the case of a path starting with a drive letter
        if std::path::Path::new(s).is_absolute() {
            return Self::parse_path(s);
        }

        match Url::parse(s) {
            Ok(url) => Ok(Self::new_with_glob(url, None)),
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

        let path = std::path::Path::new(prefix).canonicalize()?;
        let url = match path.is_file() {
            true => Url::from_file_path(path).unwrap(),
            false => Url::from_directory_path(path).unwrap(),
        };

        Ok(Self::new_with_glob(url, glob))
    }

    /// Creates a new [`ListingTableUrl`] from a url and optional glob expression
    fn new_with_glob(url: Url, glob: Option<Pattern>) -> Self {
        let prefix = Path::parse(url.path()).expect("should be URL safe");

        let pfx = prefix.clone();

        let predicate: Option<Arc<Box<dyn Fn(&ObjectMeta) -> bool + Sync + Send>>> =
            match glob {
                Some(glob) => Some(Arc::new(Box::new(move |meta| {
                    let path = &meta.location;
                    match Self::strip_prefix_x(&pfx, path) {
                        Some(mut segments) => {
                            let stripped = segments.join("/");
                            glob.matches(&stripped)
                        }
                        None => false,
                    }
                }))),
                None => None,
            };
        Self::new(url, predicate)
    }

    /// Creates a new [`ListingTableUrl`] from a url and an optional predicate/filter function
    pub fn new(
        url: Url,
        predicate: Option<Arc<Box<dyn Fn(&ObjectMeta) -> bool + Sync + Send>>>,
    ) -> Self {
        let prefix = Path::parse(url.path()).expect("should be URL safe");
        Self {
            url,
            prefix,
            predicate,
        }
    }

    /// Returns the URL scheme
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    pub(crate) fn strip_prefix_x<'a, 'b: 'a>(
        prefix: &'a Path,
        path: &'b Path,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        use object_store::path::DELIMITER;
        let path: &str = path.as_ref();
        let stripped = match prefix.as_ref() {
            "" => path,
            p => path.strip_prefix(p)?.strip_prefix(DELIMITER)?,
        };
        Some(stripped.split(DELIMITER))
    }

    /// Strips the prefix of this [`ListingTableUrl`] from the provided path, returning
    /// an iterator of the remaining path segments
    pub(crate) fn strip_prefix<'a, 'b: 'a>(
        &'a self,
        path: &'b Path,
    ) -> Option<impl Iterator<Item = &'b str> + 'a> {
        Self::strip_prefix_x(&self.prefix, path)
    }

    /// List all files identified by this [`ListingTableUrl`] for the provided `file_extension`
    pub(crate) fn list_all_files<'a>(
        &'a self,
        store: &'a dyn ObjectStore,
        file_extension: &'a str,
    ) -> BoxStream<'a, Result<ObjectMeta>> {
        // If the prefix is a file, use a head request, otherwise list
        let is_dir = self.url.as_str().ends_with('/');
        let list = match is_dir {
            true => futures::stream::once(store.list(Some(&self.prefix)))
                .try_flatten()
                .boxed(),
            false => futures::stream::once(store.head(&self.prefix)).boxed(),
        };

        list.map_err(Into::into)
            .try_filter(move |meta| {
                let path = &meta.location;
                let extension_match = path.as_ref().ends_with(file_extension);
                let predicate_ok = match &self.predicate {
                    Some(pfn) => pfn(meta),
                    None => true,
                };
                futures::future::ready(extension_match && predicate_ok)
            })
            .boxed()
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
        std::fmt::Display::fmt(&self.as_str(), f)
    }
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

/// Determines whether a file is "hidden"
#[allow(dead_code)]
pub fn is_hidden(path: &Path) -> bool {
    path.parts()
        .find(|part| part.as_ref().starts_with('.') || part.as_ref().starts_with('_'))
        .map_or_else(|| false, |_| true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use std::fs::File;
    use tempfile::tempdir;

    #[test]
    fn test_prefix_path() {
        let root = std::env::current_dir().unwrap();
        let root = root.to_string_lossy();

        let url = ListingTableUrl::parse(&root).unwrap();
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
                "testing split_glob_expression with {}",
                input
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
    fn test_is_hidden() {
        assert!(!is_hidden(&Path::parse("a").unwrap()));
        assert!(!is_hidden(&Path::parse("a/b").unwrap()));
        assert!(is_hidden(&Path::parse(".hidden").unwrap()));
        assert!(is_hidden(&Path::parse("_hidden").unwrap()));
        assert!(is_hidden(&Path::parse("a/.hidden").unwrap()));
        assert!(is_hidden(&Path::parse("a/_hidden").unwrap()));
        assert!(is_hidden(&Path::parse("a/.hidden/b").unwrap()));
        assert!(is_hidden(&Path::parse("a/_hidden/b").unwrap()));
        assert!(is_hidden(&Path::parse("a/.hidden/b").unwrap()));
    }

    #[tokio::test]
    async fn test_ltu_with_predicate() -> Result<()> {
        // wanted to use the is_hidden function in the predicate, but that doesn't work when tempdir() is something such as '/private/var/folders/8k/sn8k85w16nb1k3cjb22_fqbc0000gn/T/.tmpU33DeO/'
        let predicate: Arc<Box<dyn Fn(&ObjectMeta) -> bool + Sync + Send>> =
            Arc::new(Box::new(|meta: &ObjectMeta| {
                !meta.location.as_ref().ends_with("_SUCCESS")
            }));

        let dir = tempdir()?;
        let file_a =
            File::create(&dir.path().join("a.json")).expect("failed to create a.json");
        let file_b =
            File::create(&dir.path().join("b.json")).expect("failed to create b.json");
        let file_success = File::create(&dir.path().join("_SUCCESS"))
            .expect("failed to create _SUCCESS");

        let url = Url::from_directory_path(&dir).expect("json");
        let ltu = ListingTableUrl::new(url, Some(predicate));

        let store = LocalFileSystem::default();
        let found_files: Vec<ObjectMeta> =
            ltu.list_all_files(&store, ".json").try_collect().await?;
        assert_eq!(2, found_files.len());

        drop(file_success);
        drop(file_b);
        drop(file_a);
        dir.close()?;

        Ok(())
    }
}
