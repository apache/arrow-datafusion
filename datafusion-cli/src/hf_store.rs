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

use arrow::datatypes::ToByteSlice;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::config::{
    ConfigEntry, ConfigExtension, ConfigField, ExtensionOptions, Visit,
};
use datafusion::error::DataFusionError;
use futures::future::join_all;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use http::{header, HeaderMap, HeaderValue};
use object_store::http::{HttpBuilder, HttpStore};
use object_store::path::Path;
use object_store::{
    ClientOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload,
    PutResult, Result as ObjectStoreResult,
};
use serde::Deserialize;
use serde_json;
use snafu::{OptionExt, ResultExt, Snafu};
use std::any::Any;
use std::env;
use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;

pub const STORE: &str = "hf";
pub const DEFAULT_ENDPOINT: &str = "https://huggingface.co";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable parse source url. Url: {}, Error: {}", url, source))]
    UnableToParseUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "Unsupported schema {} in url {}, only 'hf' is supported",
        schema,
        url
    ))]
    UnsupportedUrlScheme { schema: String, url: String },

    #[snafu(display("Invalid huggingface url: {}, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'", url))]
    InvalidHfUrl { url: String },

    #[snafu(display("Unsupported repository type: {}, currently only 'datasets' or 'spaces' are supported", repo_type))]
    UnsupportedRepoType { repo_type: String },

    #[snafu(display("Unable to parse location {} into ParsedHFUrl, please format as '<repo_type>/<repository>/resolve/<revision>/<path>'", url))]
    InvalidLocation { url: String },

    #[snafu(display("Configuration key: '{}' is not known.", key))]
    UnknownConfigurationKey { key: String },

    #[snafu(display("Unable to parse tree result body, this is likely a change in the API side or a network issue, Error: {}", inner))]
    UnableToParseTreeResult { inner: serde_json::Error },

    #[snafu(display("Prefix is required for HuggingFace store"))]
    PrefixRequired,
}

impl From<Error> for ObjectStoreError {
    fn from(source: Error) -> Self {
        match source {
            Error::UnknownConfigurationKey { key } => {
                ObjectStoreError::UnknownConfigurationKey { store: STORE, key }
            }
            _ => ObjectStoreError::Generic {
                store: STORE,
                source: Box::new(source),
            },
        }
    }
}

impl From<Error> for DataFusionError {
    fn from(source: Error) -> Self {
        // Only datafusion configuration errors are exposed in this mod.
        // Other errors are aligned with generic object store errors.
        DataFusionError::Configuration(source.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ParsedHFUrl {
    path: Option<String>,
    repository: Option<String>,
    revision: Option<String>,
    repo_type: Option<String>,
}

impl Default for ParsedHFUrl {
    fn default() -> Self {
        Self {
            path: None,
            repository: None,
            revision: Some("main".to_string()),
            repo_type: Some("datasets".to_string()),
        }
    }
}

impl ParsedHFUrl {
    /// Parse a HuggingFace URL into a ParsedHFUrl struct.
    /// The URL should be in the format `hf://<repo_type>/<user|org>/<repository>[@revision]/<path>`
    /// where `repo_type` is either `datasets` or `spaces`.
    /// If the revision is not provided, it defaults to `main`.
    ///
    /// url: The HuggingFace URL to parse.
    pub fn parse_hf_style_url(url: &str) -> ObjectStoreResult<Self> {
        let url = Url::parse(url).context(UnableToParseUrlSnafu { url })?;

        if url.scheme() != "hf" {
            return Err(UnsupportedUrlSchemeSnafu {
                schema: url.scheme().to_string(),
                url: url.to_string(),
            }
            .build()
            .into());
        }

        // domain is the first part of the path, which are treated as the origin in the url.
        let repo_type = url
            .domain()
            .context(InvalidHfUrlSnafu { url: url.clone() })?;

        Self::parse_hf_style_path(repo_type, url.path())
    }

    /// Parse a HuggingFace path into a ParsedHFUrl struct.
    /// The path should be in the format `<user|org>/<repository>[@revision]/<path>` with given `repo_type`.
    /// where `repo_type` is either `datasets` or `spaces`.
    ///
    /// repo_type: The repository type, either `datasets` or `spaces`.
    /// path: The HuggingFace path to parse.
    fn parse_hf_style_path(repo_type: &str, mut path: &str) -> ObjectStoreResult<Self> {
        static EXPECTED_PARTS: usize = 3;

        let mut parsed_url = Self::default();

        if (repo_type != "datasets") && (repo_type != "spaces") {
            return Err(UnsupportedRepoTypeSnafu { repo_type }.build().into());
        }

        parsed_url.repo_type = Some(repo_type.to_string());

        // remove leading slash which is not needed.
        path = path.trim_start_matches('/');

        // parse the repository and revision.
        // - case 1: <user|org>/<repo>/<path> where <user|org>/<repo> is the repository and <repo> defaults to main.
        // - case 2: <user|org>/<repo>@<revision>/<path> where <user|org>/<repo> is the repository and <revision> is the revision.
        let path_parts = path.splitn(EXPECTED_PARTS, '/').collect::<Vec<&str>>();
        if path_parts.len() != EXPECTED_PARTS {
            return Err(InvalidHfUrlSnafu {
                url: format!("hf://{}/{}", repo_type, path),
            }
            .build()
            .into());
        }

        let revision_parts = path_parts[1].splitn(2, '@').collect::<Vec<&str>>();
        if revision_parts.len() == 2 {
            parsed_url.repository =
                Some(format!("{}/{}", path_parts[0], revision_parts[0]));
            parsed_url.revision = Some(revision_parts[1].to_string());
        } else {
            parsed_url.repository = Some(format!("{}/{}", path_parts[0], path_parts[1]));
        }

        parsed_url.path = Some(path_parts[2].to_string());

        Ok(parsed_url)
    }

    /// Parse a http style HuggingFace path into a ParsedHFUrl struct.
    /// The path should be in the format `<repo_type>/<user|org>/<repo>/resolve/<revision>/<path>`
    /// where `repo_type` is either `datasets` or `spaces`.
    ///
    /// path: The HuggingFace path to parse.
    fn parse_http_style_path(path: &str) -> ObjectStoreResult<Self> {
        static EXPECTED_PARTS: usize = 6;

        let mut parsed_url = Self::default();

        let path_parts = path.splitn(EXPECTED_PARTS, '/').collect::<Vec<&str>>();
        if path_parts.len() != EXPECTED_PARTS || path_parts[3] != "resolve" {
            return Err(InvalidLocationSnafu {
                url: path.to_string(),
            }
            .build()
            .into());
        }

        parsed_url.repo_type = Some(path_parts[0].to_string());
        parsed_url.repository = Some(format!("{}/{}", path_parts[1], path_parts[2]));
        parsed_url.revision = Some(path_parts[4].to_string());
        parsed_url.path = Some(path_parts[5].to_string());

        Ok(parsed_url)
    }

    fn to_hf_path(&self) -> String {
        let mut url = self.repository.as_deref().unwrap().to_string();

        if let Some(revision) = &self.revision {
            if revision != "main" {
                url.push('@');
                url.push_str(revision);
            }
        }

        url.push('/');
        url.push_str(self.path.as_deref().unwrap());

        url
    }

    fn to_location(&self) -> String {
        let mut url = self.to_location_dir();
        url.push('/');
        url.push_str(self.path.as_deref().unwrap());

        url
    }

    pub fn to_location_dir(&self) -> String {
        let mut url = self.repo_type.clone().unwrap();
        url.push('/');
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/resolve/");
        url.push_str(self.revision.as_deref().unwrap());

        url
    }

    pub fn to_tree_location(&self) -> String {
        let mut url = "api/".to_string();
        url.push_str(self.repo_type.as_deref().unwrap());
        url.push('/');
        url.push_str(self.repository.as_deref().unwrap());
        url.push_str("/tree/");
        url.push_str(self.revision.as_deref().unwrap());
        url.push('/');
        url.push_str(self.path.as_deref().unwrap());

        url
    }
}

/// HFOptions is the configuration options for the HFStoreBuilder.
#[derive(Debug, Clone, Default)]
pub struct HFOptions {
    endpoint: Option<String>,
    user_access_token: Option<String>,
}

impl ConfigExtension for HFOptions {
    const PREFIX: &'static str = STORE;
}

impl ExtensionOptions for HFOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let (_key, rem) = key.split_once('.').unwrap_or((key, ""));
        match rem {
            "endpoint" => {
                self.endpoint.set(rem, value)?;
            }
            "user_access_token" => {
                self.user_access_token.set(rem, value)?;
            }
            _ => {
                return Err(UnknownConfigurationKeySnafu { key }.build().into());
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        struct Visitor(Vec<ConfigEntry>);

        impl Visit for Visitor {
            fn some<V: Display>(
                &mut self,
                key: &str,
                value: V,
                description: &'static str,
            ) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: Some(value.to_string()),
                    description,
                })
            }

            fn none(&mut self, key: &str, description: &'static str) {
                self.0.push(ConfigEntry {
                    key: key.to_string(),
                    value: None,
                    description,
                })
            }
        }

        let mut v = Visitor(vec![]);
        self.endpoint
            .visit(&mut v, "endpoint", "The HuggingFace API endpoint");
        self.user_access_token.visit(
            &mut v,
            "user_access_token",
            "The HuggingFace user access token",
        );
        v.0
    }
}

pub enum HFConfigKey {
    Endpoint,
    UserAccessToken,
}

impl AsRef<str> for HFConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            Self::Endpoint => "endpoint",
            Self::UserAccessToken => "user_access_token",
        }
    }
}

impl FromStr for HFConfigKey {
    type Err = ObjectStoreError;

    fn from_str(s: &str) -> ObjectStoreResult<Self, Self::Err> {
        match s {
            "endpoint" => Ok(Self::Endpoint),
            "user_access_token" => Ok(Self::UserAccessToken),
            _ => Err(UnknownConfigurationKeySnafu { key: s.to_string() }
                .build()
                .into()),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct HFStoreBuilder {
    endpoint: Option<String>,
    repo_type: Option<String>,
    user_access_token: Option<String>,
}

impl HFStoreBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_repo_type(mut self, repo_type: impl Into<String>) -> Self {
        self.repo_type = Some(repo_type.into());
        self
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());

        self
    }

    pub fn with_user_access_token(
        mut self,
        user_access_token: impl Into<String>,
    ) -> Self {
        self.user_access_token = Some(user_access_token.into());
        self
    }

    pub fn with_config_key(mut self, key: HFConfigKey, value: impl Into<String>) -> Self {
        match key {
            HFConfigKey::Endpoint => self.endpoint = Some(value.into()),
            HFConfigKey::UserAccessToken => self.user_access_token = Some(value.into()),
        }

        self
    }

    pub fn get_config_key(&self, key: HFConfigKey) -> Option<String> {
        match key {
            HFConfigKey::Endpoint => self.endpoint.clone(),
            HFConfigKey::UserAccessToken => self.user_access_token.clone(),
        }
    }

    pub fn from_env() -> Self {
        let mut builder = Self::new();
        if let Ok(endpoint) = env::var("HF_ENDPOINT") {
            builder = builder.with_endpoint(endpoint);
        }

        if let Ok(user_access_token) = env::var("HF_USER_ACCESS_TOKEN") {
            builder = builder.with_user_access_token(user_access_token);
        }

        builder
    }

    pub fn build(&self) -> ObjectStoreResult<HFStore> {
        let mut inner_builder = HttpBuilder::new();

        let repo_type = self.repo_type.clone().unwrap_or("datasets".to_string());

        let ep;
        if let Some(endpoint) = &self.endpoint {
            ep = endpoint.to_string();
        } else {
            ep = DEFAULT_ENDPOINT.to_string();
        }

        inner_builder = inner_builder.with_url(ep.clone());

        if let Some(user_access_token) = &self.user_access_token {
            if let Ok(mut token) =
                HeaderValue::from_str(format!("Bearer {user_access_token}").as_str())
            {
                token.set_sensitive(true);

                let mut header_map = HeaderMap::new();
                header_map.insert(header::AUTHORIZATION, token);
                let options = ClientOptions::new().with_default_headers(header_map);

                inner_builder = inner_builder.with_client_options(options);
            }
        }

        let builder = inner_builder.build()?;

        Ok(HFStore::new(ep, repo_type, Arc::new(builder)))
    }
}

pub fn get_hf_object_store_builder(
    url: &Url,
    options: &HFOptions,
) -> Result<HFStoreBuilder> {
    let mut builder = HFStoreBuilder::from_env();

    // The repo type is the first part of the path, which are treated as the origin in the process.
    let Some(repo_type) = url.domain() else {
        return Err(InvalidHfUrlSnafu {
            url: url.to_string(),
        }
        .build()
        .into());
    };

    if repo_type != "datasets" && repo_type != "spaces" {
        return Err(UnsupportedRepoTypeSnafu { repo_type }.build().into());
    }

    builder = builder.with_repo_type(repo_type);

    if let Some(endpoint) = &options.endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(user_access_token) = &options.user_access_token {
        builder = builder.with_user_access_token(user_access_token);
    }

    Ok(builder)
}

#[derive(Debug)]
pub struct HFStore {
    endpoint: String,
    repo_type: String,
    store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Deserialize)]
pub struct HFTreeEntry {
    pub r#type: String,
    pub path: String,
    pub oid: String,
}

impl HFTreeEntry {
    pub fn is_file(&self) -> bool {
        self.r#type == "file"
    }
}

impl HFStore {
    pub fn new(endpoint: String, repo_type: String, store: Arc<HttpStore>) -> Self {
        Self {
            endpoint,
            repo_type,
            store,
        }
    }
}

impl Display for HFStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HFStore({})", self.endpoint)
    }
}

#[async_trait]
impl ObjectStore for HFStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        let parsed_url = ParsedHFUrl::parse_hf_style_path(
            &self.repo_type,
            location.to_string().as_str(),
        )?;

        let file_path = Path::parse(parsed_url.to_location())?;

        let mut res = self.store.get_opts(&file_path, options).await?;

        res.meta.location = location.clone();
        Ok(res)
    }

    async fn delete(&self, _location: &Path) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> ObjectStoreResult<ListResult> {
        Err(ObjectStoreError::NotImplemented)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'_, ObjectStoreResult<ObjectMeta>> {
        let Some(prefix) = prefix else {
            return futures::stream::once(async {
                Err(PrefixRequiredSnafu {}.build().into())
            })
            .boxed();
        };

        let parsed_url_result = ParsedHFUrl::parse_hf_style_path(
            &self.repo_type,
            prefix.to_string().as_str(),
        );

        if let Err(err) = parsed_url_result {
            return futures::stream::once(async { Err(err) }).boxed();
        }

        let parsed_url = parsed_url_result.unwrap();
        let tree_location = parsed_url.to_tree_location();
        let file_location_dir = parsed_url.to_location_dir();

        futures::stream::once(async move {
            let result = self.store.get(&Path::parse(tree_location)?).await?;
            let bytes = result.bytes().await?;

            let tree_result =
                serde_json::from_slice::<Vec<HFTreeEntry>>(bytes.to_byte_slice())
                    .map_err(|err| UnableToParseTreeResultSnafu { inner: err }.build())?;

            let iter = join_all(
                tree_result
                    .into_iter()
                    .filter(|entry| entry.is_file())
                    .map(|entry| format!("{}/{}", file_location_dir, entry.path.clone()))
                    .map(|meta_location| async {
                        self.store.head(&Path::parse(meta_location)?).await
                    }),
            )
            .await
            .into_iter()
            .map(|result| {
                result.and_then(|mut meta| {
                    match ParsedHFUrl::parse_http_style_path(
                        meta.location.to_string().as_str(),
                    ) {
                        Ok(parsed_url) => {
                            meta.location = Path::from_url_path(parsed_url.to_hf_path())?;
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }

                    if let Some(e_tag) = meta.e_tag.as_deref() {
                        meta.e_tag = Some(e_tag.replace('"', ""));
                    }

                    Ok(meta)
                })
            });

            Ok::<_, ObjectStoreError>(futures::stream::iter(iter))
        })
        .try_flatten()
        .boxed()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }

    async fn copy_if_not_exists(
        &self,
        _from: &Path,
        _to: &Path,
    ) -> ObjectStoreResult<()> {
        Err(ObjectStoreError::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use crate::hf_store::{HFConfigKey, HFOptions, HFStoreBuilder, ParsedHFUrl};

    #[test]
    fn test_parse_hf_url() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "main";
        let path = "data.csv";

        let url = format!("hf://{}/{}/{}", repo_type, repository, path);

        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(parsed_url.repo_type, Some(repo_type.to_string()));
        assert_eq!(parsed_url.repository, Some(repository.to_string()));
        assert_eq!(parsed_url.revision, Some(revision.to_string()));
        assert_eq!(parsed_url.path, Some(path.to_string()));

        let hf_path = format!("{}/{}", repository, path);
        let parsed_path_url =
            ParsedHFUrl::parse_hf_style_path(repo_type, &hf_path).unwrap();

        assert_eq!(parsed_path_url.repo_type, parsed_url.repo_type);
        assert_eq!(parsed_path_url.repository, parsed_url.repository);
        assert_eq!(parsed_path_url.revision, parsed_url.revision);
        assert_eq!(parsed_path_url.path, parsed_url.path);
    }

    #[test]
    fn test_parse_hf_url_with_revision() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "~parquet";
        let path = "data.csv";

        let url = format!("hf://{}/{}@{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(parsed_url.repo_type, Some(repo_type.to_string()));
        assert_eq!(parsed_url.repository, Some(repository.to_string()));
        assert_eq!(parsed_url.revision, Some(revision.to_string()));
        assert_eq!(parsed_url.path, Some(path.to_string()));

        let hf_path = format!("{}@{}/{}", repository, revision, path);
        let parsed_path_url =
            ParsedHFUrl::parse_hf_style_path(repo_type, &hf_path).unwrap();

        assert_eq!(parsed_path_url.repo_type, parsed_url.repo_type);
        assert_eq!(parsed_path_url.repository, parsed_url.repository);
        assert_eq!(parsed_path_url.revision, parsed_url.revision);
        assert_eq!(parsed_path_url.path, parsed_url.path);
    }

    #[test]
    fn test_parse_hf_url_error() {
        test_parse_hf_url_error_matches(
            "abc",
            "Generic hf error: Unable parse source url. Url: abc, Error: relative URL without a base"
        );

        test_parse_hf_url_error_matches(
            "hf://",
            "Generic hf error: Invalid huggingface url: hf://, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'"
        );

        test_parse_hf_url_error_matches(
            "df://datasets/datasets-examples/doc-formats-csv-1",
            "Generic hf error: Unsupported schema df in url df://datasets/datasets-examples/doc-formats-csv-1, only 'hf' is supported"
        );

        test_parse_hf_url_error_matches(
            "hf://datadicts/datasets-examples/doc-formats-csv-1/data.csv",
            "Generic hf error: Unsupported repository type: datadicts, currently only 'datasets' or 'spaces' are supported"
        );

        test_parse_hf_url_error_matches(
            "hf://datasets/datasets-examples/doc-formats-csv-1",
            "Generic hf error: Invalid huggingface url: hf://datasets/datasets-examples/doc-formats-csv-1, please format as 'hf://<repo_type>/<repository>[@revision]/<path>'"
        );
    }

    fn test_parse_hf_url_error_matches(url: &str, expected_error: &str) {
        let parsed_url_result = ParsedHFUrl::parse_hf_style_url(url);

        assert!(parsed_url_result.is_err());
        assert_eq!(parsed_url_result.unwrap_err().to_string(), expected_error);
    }

    #[test]
    fn test_parse_http_url() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "main";
        let path = "data.csv";

        let url = format!("{}/{}/resolve/{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_http_style_path(&url).unwrap();

        assert_eq!(parsed_url.repo_type, Some(repo_type.to_string()));
        assert_eq!(parsed_url.repository, Some(repository.to_string()));
        assert_eq!(parsed_url.revision, Some(revision.to_string()));
        assert_eq!(parsed_url.path, Some(path.to_string()));
    }

    #[test]
    fn test_parse_http_url_with_revision() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "~parquet";
        let path = "data.csv";

        let url = format!("{}/{}/resolve/{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_http_style_path(&url).unwrap();

        assert_eq!(parsed_url.repo_type, Some(repo_type.to_string()));
        assert_eq!(parsed_url.repository, Some(repository.to_string()));
        assert_eq!(parsed_url.revision, Some(revision.to_string()));
        assert_eq!(parsed_url.path, Some(path.to_string()));
    }

    #[test]
    fn test_parse_http_url_error() {
        test_parse_http_url_error_matches(
            "datasets/datasets-examples/doc-formats-csv-1",
            "Generic hf error: Unable to parse location datasets/datasets-examples/doc-formats-csv-1 into ParsedHFUrl, please format as '<repo_type>/<repository>/resolve/<revision>/<path>'"
        );

        test_parse_http_url_error_matches(
            "datasets/datasets-examples/doc-formats-csv-1/data.csv",
            "Generic hf error: Unable to parse location datasets/datasets-examples/doc-formats-csv-1/data.csv into ParsedHFUrl, please format as '<repo_type>/<repository>/resolve/<revision>/<path>'"
        );

        test_parse_http_url_error_matches(
            "datasets/datasets-examples/doc-formats-csv-1/resolve/main",
            "Generic hf error: Unable to parse location datasets/datasets-examples/doc-formats-csv-1/resolve/main into ParsedHFUrl, please format as '<repo_type>/<repository>/resolve/<revision>/<path>'"
        );
    }

    fn test_parse_http_url_error_matches(url: &str, expected_error: &str) {
        let parsed_url_result = ParsedHFUrl::parse_http_style_path(url);
        assert!(parsed_url_result.is_err());
        assert_eq!(parsed_url_result.unwrap_err().to_string(), expected_error);
    }

    #[test]
    fn test_to_hf_path() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let path = "data.csv";

        let url = format!("hf://{}/{}/{}", repo_type, repository, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(parsed_url.to_hf_path(), format!("{}/{}", repository, path));

        let revision = "~parquet";
        let url = format!("hf://{}/{}@{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_hf_path(),
            format!("{}@{}/{}", repository, revision, path)
        );
    }

    #[test]
    fn test_to_location() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "main";
        let path = "data.csv";

        let url = format!("hf://{}/{}/{}", repo_type, repository, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_location(),
            format!("{}/{}/resolve/{}/{}", repo_type, repository, revision, path)
        );

        let revision = "~parquet";
        let url = format!("hf://{}/{}@{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_location(),
            format!("{}/{}/resolve/{}/{}", repo_type, repository, revision, path)
        );
    }

    #[test]
    fn test_to_location_dir() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "main";
        let path = "data.csv";

        let url = format!("hf://{}/{}/{}", repo_type, repository, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_location_dir(),
            format!("{}/{}/resolve/{}", repo_type, repository, revision)
        );

        let revision = "~parquet";
        let url = format!("hf://{}/{}@{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_location_dir(),
            format!("{}/{}/resolve/{}", repo_type, repository, revision)
        );
    }

    #[test]
    fn test_to_tree_location() {
        let repo_type = "datasets";
        let repository = "datasets-examples/doc-formats-csv-1";
        let revision = "main";
        let path = "data.csv";

        let url = format!("hf://{}/{}/{}", repo_type, repository, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_tree_location(),
            format!(
                "api/{}/{}/tree/{}/{}",
                repo_type, repository, revision, path
            )
        );

        let revision = "~parquet";
        let url = format!("hf://{}/{}@{}/{}", repo_type, repository, revision, path);
        let parsed_url = ParsedHFUrl::parse_hf_style_url(url.as_str()).unwrap();

        assert_eq!(
            parsed_url.to_tree_location(),
            format!(
                "api/{}/{}/tree/{}/{}",
                repo_type, repository, revision, path
            )
        );
    }

    #[test]
    fn test_hf_store_builder() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "abc";

        let builder = HFStoreBuilder::new()
            .with_endpoint(endpoint)
            .with_user_access_token(user_access_token);

        assert_eq!(builder.endpoint, Some(endpoint.to_string()));
        assert_eq!(
            builder.user_access_token,
            Some(user_access_token.to_string())
        );
    }

    #[test]
    fn test_hf_store_builder_default() {
        let builder = HFStoreBuilder::new();

        assert_eq!(builder.endpoint, None);
        assert_eq!(builder.user_access_token, None);
    }

    #[test]
    fn test_fn_store_from_config_key() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "abc";

        let builder = HFStoreBuilder::new()
            .with_config_key(HFConfigKey::Endpoint, endpoint)
            .with_config_key(HFConfigKey::UserAccessToken, user_access_token);

        assert_eq!(builder.endpoint, Some(endpoint.to_string()));
        assert_eq!(
            builder.user_access_token,
            Some(user_access_token.to_string())
        );
    }

    #[test]
    fn test_hf_store_builder_from_env() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "abc";

        std::env::set_var("HF_ENDPOINT", endpoint);
        std::env::set_var("HF_USER_ACCESS_TOKEN", user_access_token);

        let builder = HFStoreBuilder::from_env();

        assert_eq!(builder.endpoint, Some(endpoint.to_string()));
        assert_eq!(
            builder.user_access_token,
            Some(user_access_token.to_string())
        );
    }

    #[test]
    fn test_hf_store_builder_preserve_case() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "AbcD231_!@#";

        let builder = HFStoreBuilder::new()
            .with_config_key(HFConfigKey::Endpoint, endpoint)
            .with_config_key(HFConfigKey::UserAccessToken, user_access_token);

        assert_eq!(builder.endpoint, Some(endpoint.to_string()));
        assert_eq!(
            builder.user_access_token,
            Some(user_access_token.to_string())
        );
    }

    #[test]
    fn test_get_hf_object_store_builder() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "abc";

        let url =
            url::Url::parse("hf://datasets/datasets-examples/doc-formats-csv-1/data.csv")
                .unwrap();
        let options = HFOptions {
            endpoint: Some(endpoint.to_string()),
            user_access_token: Some(user_access_token.to_string()),
        };

        let builder = super::get_hf_object_store_builder(&url, &options).unwrap();

        assert_eq!(builder.endpoint, Some(endpoint.to_string()));
        assert_eq!(
            builder.user_access_token,
            Some(user_access_token.to_string())
        );
    }

    #[test]
    fn test_get_hf_object_store_builder_error() {
        let endpoint = "https://huggingface.co";
        let user_access_token = "abc";

        let url = url::Url::parse(
            "hf://datadicts/datasets-examples/doc-formats-csv-1/data.csv",
        )
        .unwrap();
        let options = HFOptions {
            endpoint: Some(endpoint.to_string()),
            user_access_token: Some(user_access_token.to_string()),
        };

        let expected_error = super::get_hf_object_store_builder(&url, &options);
        assert!(expected_error.is_err());

        let expected_error = expected_error.unwrap_err();
        assert_eq!(
            expected_error.to_string(),
            "Invalid or Unsupported Configuration: Unsupported repository type: datadicts, currently only 'datasets' or 'spaces' are supported"
        );
    }
}
