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

//! Runtime configuration, via [`ConfigOptions`]

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::str::FromStr;

use crate::error::_config_err;
use crate::parsers::CompressionTypeVariant;
use crate::{DataFusionError, Result};

/// A macro that wraps a configuration struct and automatically derives
/// [`Default`] and [`ConfigField`] for it, allowing it to be used
/// in the [`ConfigOptions`] configuration tree
///
/// For example,
///
/// ```ignore
/// config_namespace! {
///    /// Amazing config
///    pub struct MyConfig {
///        /// Field 1 doc
///        field1: String, default = "".to_string()
///
///        /// Field 2 doc
///        field2: usize, default = 232
///
///        /// Field 3 doc
///        field3: Option<usize>, default = None
///    }
///}
/// ```
///
/// Will generate
///
/// ```ignore
/// /// Amazing config
/// #[derive(Debug, Clone)]
/// #[non_exhaustive]
/// pub struct MyConfig {
///     /// Field 1 doc
///     field1: String,
///     /// Field 2 doc
///     field2: usize,
///     /// Field 3 doc
///     field3: Option<usize>,
/// }
/// impl ConfigField for MyConfig {
///     fn set(&mut self, key: &str, value: &str) -> Result<()> {
///         let (key, rem) = key.split_once('.').unwrap_or((key, ""));
///         match key {
///             "field1" => self.field1.set(rem, value),
///             "field2" => self.field2.set(rem, value),
///             "field3" => self.field3.set(rem, value),
///             _ => _internal_err!(
///                 "Config value \"{}\" not found on MyConfig",
///                 key
///             ),
///         }
///     }
///
///     fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
///         let key = format!("{}.field1", key_prefix);
///         let desc = "Field 1 doc";
///         self.field1.visit(v, key.as_str(), desc);
///         let key = format!("{}.field2", key_prefix);
///         let desc = "Field 2 doc";
///         self.field2.visit(v, key.as_str(), desc);
///         let key = format!("{}.field3", key_prefix);
///         let desc = "Field 3 doc";
///         self.field3.visit(v, key.as_str(), desc);
///     }
/// }
///
/// impl Default for MyConfig {
///     fn default() -> Self {
///         Self {
///             field1: "".to_string(),
///             field2: 232,
///             field3: None,
///         }
///     }
/// }
/// ```
///
/// NB: Misplaced commas may result in nonsensical errors
///
#[macro_export]
macro_rules! config_namespace {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {

        $(#[doc = $struct_d])*
        #[derive(Debug, Clone, PartialEq)]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                       stringify!($field_name) => self.$field_name.set(rem, value),
                    )*
                    _ => return _config_err!(
                        "Config value \"{}\" not found on {}", key, stringify!($struct_name)
                    )
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                let desc = concat!($($d),*).trim();
                self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }
    }
}

config_namespace! {
    /// Options related to catalog and directory scanning
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct CatalogOptions {
        /// Whether the default catalog and schema should be created automatically.
        pub create_default_catalog_and_schema: bool, default = true

        /// The default catalog name - this impacts what SQL queries use if not specified
        pub default_catalog: String, default = "datafusion".to_string()

        /// The default schema name - this impacts what SQL queries use if not specified
        pub default_schema: String, default = "public".to_string()

        /// Should DataFusion provide access to `information_schema`
        /// virtual tables for displaying schema information
        pub information_schema: bool, default = false

        /// Location scanned to load tables for `default` schema
        pub location: Option<String>, default = None

        /// Type of `TableProvider` to use when loading `default` schema
        pub format: Option<String>, default = None

        /// Default value for `format.has_header` for `CREATE EXTERNAL TABLE`
        /// if not specified explicitly in the statement.
        pub has_header: bool, default = false
    }
}

config_namespace! {
    /// Options related to SQL parser
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct SqlParserOptions {
        /// When set to true, SQL parser will parse float as decimal type
        pub parse_float_as_decimal: bool, default = false

        /// When set to true, SQL parser will normalize ident (convert ident to lowercase when not quoted)
        pub enable_ident_normalization: bool, default = true

        /// Configure the SQL dialect used by DataFusion's parser; supported values include: Generic,
        /// MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, MsSQL, ClickHouse, BigQuery, and Ansi.
        pub dialect: String, default = "generic".to_string()

        /// If true, permit lengths for `VARCHAR` such as `VARCHAR(20)`, but
        /// ignore the length. If false, error if a `VARCHAR` with a length is
        /// specified. The Arrow type system does not have a notion of maximum
        /// string length and thus DataFusion can not enforce such limits.
        pub support_varchar_with_length: bool, default = true
    }
}

config_namespace! {
    /// Options related to query execution
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ExecutionOptions {
        /// Default batch size while creating new batches, it's especially useful for
        /// buffer-in-memory batches since creating tiny batches would result in too much
        /// metadata memory consumption
        pub batch_size: usize, default = 8192

        /// When set to true, record batches will be examined between each operator and
        /// small batches will be coalesced into larger batches. This is helpful when there
        /// are highly selective filters or joins that could produce tiny output batches. The
        /// target batch size is determined by the configuration setting
        pub coalesce_batches: bool, default = true

        /// Should DataFusion collect statistics after listing files
        pub collect_statistics: bool, default = false

        /// Number of partitions for query execution. Increasing partitions can increase
        /// concurrency.
        ///
        /// Defaults to the number of CPU cores on the system
        pub target_partitions: usize, default = num_cpus::get()

        /// The default time zone
        ///
        /// Some functions, e.g. `EXTRACT(HOUR from SOME_TIME)`, shift the underlying datetime
        /// according to this time zone, and then extract the hour
        pub time_zone: Option<String>, default = Some("+00:00".into())

        /// Parquet options
        pub parquet: ParquetOptions, default = Default::default()

        /// Aggregate options
        pub aggregate: AggregateOptions, default = Default::default()

        /// Fan-out during initial physical planning.
        ///
        /// This is mostly use to plan `UNION` children in parallel.
        ///
        /// Defaults to the number of CPU cores on the system
        pub planning_concurrency: usize, default = num_cpus::get()

        /// Specifies the reserved memory for each spillable sort operation to
        /// facilitate an in-memory merge.
        ///
        /// When a sort operation spills to disk, the in-memory data must be
        /// sorted and merged before being written to a file. This setting reserves
        /// a specific amount of memory for that in-memory sort/merge process.
        ///
        /// Note: This setting is irrelevant if the sort operation cannot spill
        /// (i.e., if there's no `DiskManager` configured).
        pub sort_spill_reservation_bytes: usize, default = 10 * 1024 * 1024

        /// When sorting, below what size should data be concatenated
        /// and sorted in a single RecordBatch rather than sorted in
        /// batches and merged.
        pub sort_in_place_threshold_bytes: usize, default = 1024 * 1024

        /// Number of files to read in parallel when inferring schema and statistics
        pub meta_fetch_concurrency: usize, default = 32

        /// Guarantees a minimum level of output files running in parallel.
        /// RecordBatches will be distributed in round robin fashion to each
        /// parallel writer. Each writer is closed and a new file opened once
        /// soft_max_rows_per_output_file is reached.
        pub minimum_parallel_output_files: usize, default = 4

        /// Target number of rows in output files when writing multiple.
        /// This is a soft max, so it can be exceeded slightly. There also
        /// will be one file smaller than the limit if the total
        /// number of rows written is not roughly divisible by the soft max
        pub soft_max_rows_per_output_file: usize, default = 50000000

        /// This is the maximum number of RecordBatches buffered
        /// for each output file being worked. Higher values can potentially
        /// give faster write performance at the cost of higher peak
        /// memory consumption
        pub max_buffered_batches_per_output_file: usize, default = 2

        /// Should sub directories be ignored when scanning directories for data
        /// files. Defaults to true (ignores subdirectories), consistent with
        /// Hive. Note that this setting does not affect reading partitioned
        /// tables (e.g. `/table/year=2021/month=01/data.parquet`).
        pub listing_table_ignore_subdirectory: bool, default = true

        /// Should DataFusion support recursive CTEs
        pub enable_recursive_ctes: bool, default = true

        /// Attempt to eliminate sorts by packing & sorting files with non-overlapping
        /// statistics into the same file groups.
        /// Currently experimental
        pub split_file_groups_by_statistics: bool, default = false

        /// Should Datafusion keep the columns used for partition_by in the output RecordBatches
        pub keep_partition_by_columns: bool, default = false
    }
}

config_namespace! {
    /// Options related to parquet files
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ParquetOptions {
        /// If true, reads the Parquet data page level metadata (the
        /// Page Index), if present, to reduce the I/O and number of
        /// rows decoded.
        pub enable_page_index: bool, default = true

        /// If true, the parquet reader attempts to skip entire row groups based
        /// on the predicate in the query and the metadata (min/max values) stored in
        /// the parquet file
        pub pruning: bool, default = true

        /// If true, the parquet reader skip the optional embedded metadata that may be in
        /// the file Schema. This setting can help avoid schema conflicts when querying
        /// multiple parquet files with schemas containing compatible types but different metadata
        pub skip_metadata: bool, default = true

        /// If specified, the parquet reader will try and fetch the last `size_hint`
        /// bytes of the parquet file optimistically. If not specified, two reads are required:
        /// One read to fetch the 8-byte parquet footer and
        /// another to fetch the metadata length encoded in the footer
        pub metadata_size_hint: Option<usize>, default = None

        /// If true, filter expressions are be applied during the parquet decoding operation to
        /// reduce the number of rows decoded. This optimization is sometimes called "late materialization".
        pub pushdown_filters: bool, default = false

        /// If true, filter expressions evaluated during the parquet decoding operation
        /// will be reordered heuristically to minimize the cost of evaluation. If false,
        /// the filters are applied in the same order as written in the query
        pub reorder_filters: bool, default = false

        // The following map to parquet::file::properties::WriterProperties

        /// Sets best effort maximum size of data page in bytes
        pub data_pagesize_limit: usize, default = 1024 * 1024

        /// Sets write_batch_size in bytes
        pub write_batch_size: usize, default = 1024

        /// Sets parquet writer version
        /// valid values are "1.0" and "2.0"
        pub writer_version: String, default = "1.0".into()

        /// Sets default parquet compression codec
        /// Valid values are: uncompressed, snappy, gzip(level),
        /// lzo, brotli(level), lz4, zstd(level), and lz4_raw.
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        pub compression: Option<String>, default = Some("zstd(3)".into())

        /// Sets if dictionary encoding is enabled. If NULL, uses
        /// default parquet writer setting
        pub dictionary_enabled: Option<bool>, default = None

        /// Sets best effort maximum dictionary page size, in bytes
        pub dictionary_page_size_limit: usize, default = 1024 * 1024

        /// Sets if statistics are enabled for any column
        /// Valid values are: "none", "chunk", and "page"
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        pub statistics_enabled: Option<String>, default = None

        /// Sets max statistics size for any column. If NULL, uses
        /// default parquet writer setting
        pub max_statistics_size: Option<usize>, default = None

        /// Target maximum number of rows in each row group (defaults to 1M
        /// rows). Writing larger row groups requires more memory to write, but
        /// can get better compression and be faster to read.
        pub max_row_group_size: usize, default = 1024 * 1024

        /// Sets "created by" property
        pub created_by: String, default = concat!("datafusion version ", env!("CARGO_PKG_VERSION")).into()

        /// Sets column index truncate length
        pub column_index_truncate_length: Option<usize>, default = None

        /// Sets best effort maximum number of rows in data page
        pub data_page_row_count_limit: usize, default = usize::MAX

        /// Sets default encoding for any column
        /// Valid values are: plain, plain_dictionary, rle,
        /// bit_packed, delta_binary_packed, delta_length_byte_array,
        /// delta_byte_array, rle_dictionary, and byte_stream_split.
        /// These values are not case sensitive. If NULL, uses
        /// default parquet writer setting
        pub encoding: Option<String>, default = None

        /// Use any available bloom filters when reading parquet files
        pub bloom_filter_on_read: bool, default = true

        /// Write bloom filters for all columns when creating parquet files
        pub bloom_filter_on_write: bool, default = false

        /// Sets bloom filter false positive probability. If NULL, uses
        /// default parquet writer setting
        pub bloom_filter_fpp: Option<f64>, default = None

        /// Sets bloom filter number of distinct values. If NULL, uses
        /// default parquet writer setting
        pub bloom_filter_ndv: Option<u64>, default = None

        /// Controls whether DataFusion will attempt to speed up writing
        /// parquet files by serializing them in parallel. Each column
        /// in each row group in each output file are serialized in parallel
        /// leveraging a maximum possible core count of n_files*n_row_groups*n_columns.
        pub allow_single_file_parallelism: bool, default = true

        /// By default parallel parquet writer is tuned for minimum
        /// memory usage in a streaming execution plan. You may see
        /// a performance benefit when writing large parquet files
        /// by increasing maximum_parallel_row_group_writers and
        /// maximum_buffered_record_batches_per_stream if your system
        /// has idle cores and can tolerate additional memory usage.
        /// Boosting these values is likely worthwhile when
        /// writing out already in-memory data, such as from a cached
        /// data frame.
        pub maximum_parallel_row_group_writers: usize, default = 1

        /// By default parallel parquet writer is tuned for minimum
        /// memory usage in a streaming execution plan. You may see
        /// a performance benefit when writing large parquet files
        /// by increasing maximum_parallel_row_group_writers and
        /// maximum_buffered_record_batches_per_stream if your system
        /// has idle cores and can tolerate additional memory usage.
        /// Boosting these values is likely worthwhile when
        /// writing out already in-memory data, such as from a cached
        /// data frame.
        pub maximum_buffered_record_batches_per_stream: usize, default = 2

    }
}

config_namespace! {
    /// Options related to aggregate execution
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct AggregateOptions {
        /// Specifies the threshold for using `ScalarValue`s to update
        /// accumulators during high-cardinality aggregations for each input batch.
        ///
        /// The aggregation is considered high-cardinality if the number of affected groups
        /// is greater than or equal to `batch_size / scalar_update_factor`. In such cases,
        /// `ScalarValue`s are utilized for updating accumulators, rather than the default
        /// batch-slice approach. This can lead to performance improvements.
        ///
        /// By adjusting the `scalar_update_factor`, you can balance the trade-off between
        /// more efficient accumulator updates and the number of groups affected.
        pub scalar_update_factor: usize, default = 10
    }
}

config_namespace! {
    /// Options related to query optimization
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct OptimizerOptions {
        /// When set to true, the optimizer will push a limit operation into
        /// grouped aggregations which have no aggregate expressions, as a soft limit,
        /// emitting groups once the limit is reached, before all rows in the group are read.
        pub enable_distinct_aggregation_soft_limit: bool, default = true

        /// When set to true, the physical plan optimizer will try to add round robin
        /// repartitioning to increase parallelism to leverage more CPU cores
        pub enable_round_robin_repartition: bool, default = true

        /// When set to true, the optimizer will attempt to perform limit operations
        /// during aggregations, if possible
        pub enable_topk_aggregation: bool, default = true

        /// When set to true, the optimizer will insert filters before a join between
        /// a nullable and non-nullable column to filter out nulls on the nullable side. This
        /// filter can add additional overhead when the file format does not fully support
        /// predicate push down.
        pub filter_null_join_keys: bool, default = false

        /// Should DataFusion repartition data using the aggregate keys to execute aggregates
        /// in parallel using the provided `target_partitions` level
        pub repartition_aggregations: bool, default = true

        /// Minimum total files size in bytes to perform file scan repartitioning.
        pub repartition_file_min_size: usize, default = 10 * 1024 * 1024

        /// Should DataFusion repartition data using the join keys to execute joins in parallel
        /// using the provided `target_partitions` level
        pub repartition_joins: bool, default = true

        /// Should DataFusion allow symmetric hash joins for unbounded data sources even when
        /// its inputs do not have any ordering or filtering If the flag is not enabled,
        /// the SymmetricHashJoin operator will be unable to prune its internal buffers,
        /// resulting in certain join types - such as Full, Left, LeftAnti, LeftSemi, Right,
        /// RightAnti, and RightSemi - being produced only at the end of the execution.
        /// This is not typical in stream processing. Additionally, without proper design for
        /// long runner execution, all types of joins may encounter out-of-memory errors.
        pub allow_symmetric_joins_without_pruning: bool, default = true

        /// When set to `true`, file groups will be repartitioned to achieve maximum parallelism.
        /// Currently Parquet and CSV formats are supported.
        ///
        /// If set to `true`, all files will be repartitioned evenly (i.e., a single large file
        /// might be partitioned into smaller chunks) for parallel scanning.
        /// If set to `false`, different files will be read in parallel, but repartitioning won't
        /// happen within a single file.
        pub repartition_file_scans: bool, default = true

        /// Should DataFusion repartition data using the partitions keys to execute window
        /// functions in parallel using the provided `target_partitions` level
        pub repartition_windows: bool, default = true

        /// Should DataFusion execute sorts in a per-partition fashion and merge
        /// afterwards instead of coalescing first and sorting globally.
        /// With this flag is enabled, plans in the form below
        ///
        /// ```text
        ///      "SortExec: [a@0 ASC]",
        ///      "  CoalescePartitionsExec",
        ///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        /// ```
        /// would turn into the plan below which performs better in multithreaded environments
        ///
        /// ```text
        ///      "SortPreservingMergeExec: [a@0 ASC]",
        ///      "  SortExec: [a@0 ASC]",
        ///      "    RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
        /// ```
        pub repartition_sorts: bool, default = true

        /// When true, DataFusion will opportunistically remove sorts when the data is already sorted,
        /// (i.e. setting `preserve_order` to true on `RepartitionExec`  and
        /// using `SortPreservingMergeExec`)
        ///
        /// When false, DataFusion will maximize plan parallelism using
        /// `RepartitionExec` even if this requires subsequently resorting data using a `SortExec`.
        pub prefer_existing_sort: bool, default = false

        /// When set to true, the logical plan optimizer will produce warning
        /// messages if any optimization rules produce errors and then proceed to the next
        /// rule. When set to false, any rules that produce errors will cause the query to fail
        pub skip_failed_rules: bool, default = false

        /// Number of times that the optimizer will attempt to optimize the plan
        pub max_passes: usize, default = 3

        /// When set to true, the physical plan optimizer will run a top down
        /// process to reorder the join keys
        pub top_down_join_key_reordering: bool, default = true

        /// When set to true, the physical plan optimizer will prefer HashJoin over SortMergeJoin.
        /// HashJoin can work more efficiently than SortMergeJoin but consumes more memory
        pub prefer_hash_join: bool, default = true

        /// The maximum estimated size in bytes for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold: usize, default = 1024 * 1024

        /// The maximum estimated size in rows for one input side of a HashJoin
        /// will be collected into a single partition
        pub hash_join_single_partition_threshold_rows: usize, default = 1024 * 128

        /// The default filter selectivity used by Filter Statistics
        /// when an exact selectivity cannot be determined. Valid values are
        /// between 0 (no selectivity) and 100 (all rows are selected).
        pub default_filter_selectivity: u8, default = 20

        /// When set to true, the optimizer will not attempt to convert Union to Interleave
        pub prefer_existing_union: bool, default = false


        /// Number of input streams to prefetch for ProgressiveEvalExec
        /// Since ProgressiveEvalExec only polls one stream at a time in their stream order,
        /// we do not need to prefetch all streams at once to save resources. However, if the
        /// streams' IO time is way more than their CPU/procesing time, prefetching them will help
        /// improve the performance.
        /// Default is 2 which means we will prefetch one extra stream before polling the current one.
        /// Increasing this value if IO time to read a stream is often much more than CPU time to process its previous one.
        pub progressive_eval_num_prefetch_input_streams: usize, default = 2
    }
}

config_namespace! {
    /// Options controlling explain output
    ///
    /// See also: [`SessionConfig`]
    ///
    /// [`SessionConfig`]: https://docs.rs/datafusion/latest/datafusion/prelude/struct.SessionConfig.html
    pub struct ExplainOptions {
        /// When set to true, the explain statement will only print logical plans
        pub logical_plan_only: bool, default = false

        /// When set to true, the explain statement will only print physical plans
        pub physical_plan_only: bool, default = false

        /// When set to true, the explain statement will print operator statistics
        /// for physical plans
        pub show_statistics: bool, default = false

        /// When set to true, the explain statement will print the partition sizes
        pub show_sizes: bool, default = true

        /// When set to true, the explain statement will print schema information
        pub show_schema: bool, default = false
    }
}

/// A key value pair, with a corresponding description
#[derive(Debug)]
pub struct ConfigEntry {
    /// A unique string to identify this config value
    pub key: String,

    /// The value if any
    pub value: Option<String>,

    /// A description of this configuration entry
    pub description: &'static str,
}

/// Configuration options struct, able to store both built-in configuration and custom options
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ConfigOptions {
    /// Catalog options
    pub catalog: CatalogOptions,
    /// Execution options
    pub execution: ExecutionOptions,
    /// Optimizer options
    pub optimizer: OptimizerOptions,
    /// SQL parser options
    pub sql_parser: SqlParserOptions,
    /// Explain options
    pub explain: ExplainOptions,
    /// Optional extensions registered using [`Extensions::insert`]
    pub extensions: Extensions,
}

impl ConfigField for ConfigOptions {
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Extensions are handled in the public `ConfigOptions::set`
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        match key {
            "catalog" => self.catalog.set(rem, value),
            "execution" => self.execution.set(rem, value),
            "optimizer" => self.optimizer.set(rem, value),
            "explain" => self.explain.set(rem, value),
            "sql_parser" => self.sql_parser.set(rem, value),
            _ => _config_err!("Config value \"{key}\" not found on ConfigOptions"),
        }
    }

    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        self.catalog.visit(v, "datafusion.catalog", "");
        self.execution.visit(v, "datafusion.execution", "");
        self.optimizer.visit(v, "datafusion.optimizer", "");
        self.explain.visit(v, "datafusion.explain", "");
        self.sql_parser.visit(v, "datafusion.sql_parser", "");
    }
}

impl ConfigOptions {
    /// Creates a new [`ConfigOptions`] with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set extensions to provided value
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Set a configuration option
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let Some((prefix, key)) = key.split_once('.') else {
            return _config_err!("could not find config namespace for key \"{key}\"");
        };

        if prefix == "datafusion" {
            return ConfigField::set(self, key, value);
        }

        let Some(e) = self.extensions.0.get_mut(prefix) else {
            return _config_err!("Could not find config namespace \"{prefix}\"");
        };
        e.0.set(key, value)
    }

    /// Create new ConfigOptions struct, taking values from
    /// environment variables where possible.
    ///
    /// For example, setting `DATAFUSION_EXECUTION_BATCH_SIZE` will
    /// control `datafusion.execution.batch_size`.
    pub fn from_env() -> Result<Self> {
        struct Visitor(Vec<String>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, _: V, _: &'static str) {
                self.0.push(key.to_string())
            }

            fn none(&mut self, key: &str, _: &'static str) {
                self.0.push(key.to_string())
            }
        }

        // Extract the names of all fields and then look up the corresponding
        // environment variables. This isn't hugely efficient but avoids
        // ambiguity between `a.b` and `a_b` which would both correspond
        // to an environment variable of `A_B`

        let mut keys = Visitor(vec![]);
        let mut ret = Self::default();
        ret.visit(&mut keys, "datafusion", "");

        for key in keys.0 {
            let env = key.to_uppercase().replace('.', "_");
            if let Some(var) = std::env::var_os(env) {
                ret.set(&key, var.to_string_lossy().as_ref())?;
            }
        }

        Ok(ret)
    }

    /// Create new ConfigOptions struct, taking values from a string hash map.
    ///
    /// Only the built-in configurations will be extracted from the hash map
    /// and other key value pairs will be ignored.
    pub fn from_string_hash_map(settings: HashMap<String, String>) -> Result<Self> {
        struct Visitor(Vec<String>);

        impl Visit for Visitor {
            fn some<V: Display>(&mut self, key: &str, _: V, _: &'static str) {
                self.0.push(key.to_string())
            }

            fn none(&mut self, key: &str, _: &'static str) {
                self.0.push(key.to_string())
            }
        }

        let mut keys = Visitor(vec![]);
        let mut ret = Self::default();
        ret.visit(&mut keys, "datafusion", "");

        for key in keys.0 {
            if let Some(var) = settings.get(&key) {
                ret.set(&key, var)?;
            }
        }

        Ok(ret)
    }

    /// Returns the [`ConfigEntry`] stored within this [`ConfigOptions`]
    pub fn entries(&self) -> Vec<ConfigEntry> {
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
        self.visit(&mut v, "datafusion", "");

        v.0.extend(self.extensions.0.values().flat_map(|e| e.0.entries()));
        v.0
    }

    /// Generate documentation that can be included in the user guide
    pub fn generate_config_markdown() -> String {
        use std::fmt::Write as _;

        let mut s = Self::default();

        // Normalize for display
        s.execution.target_partitions = 0;
        s.execution.planning_concurrency = 0;

        let mut docs = "| key | default | description |\n".to_string();
        docs += "|-----|---------|-------------|\n";
        let mut entries = s.entries();
        entries.sort_unstable_by(|a, b| a.key.cmp(&b.key));

        for entry in s.entries() {
            let _ = writeln!(
                &mut docs,
                "| {} | {} | {} |",
                entry.key,
                entry.value.as_deref().unwrap_or("NULL"),
                entry.description
            );
        }
        docs
    }
}

/// [`ConfigExtension`] provides a mechanism to store third-party configuration within DataFusion
///
/// Unfortunately associated constants are not currently object-safe, and so this
/// extends the object-safe [`ExtensionOptions`]
pub trait ConfigExtension: ExtensionOptions {
    /// Configuration namespace prefix to use
    ///
    /// All values under this will be prefixed with `$PREFIX + "."`
    const PREFIX: &'static str;
}

/// An object-safe API for storing arbitrary configuration
pub trait ExtensionOptions: Send + Sync + std::fmt::Debug + 'static {
    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilised
    fn as_any(&self) -> &dyn Any;

    /// Return `self` as [`Any`]
    ///
    /// This is needed until trait upcasting is stabilised
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Return a deep clone of this [`ExtensionOptions`]
    ///
    /// It is important this does not share mutable state to avoid consistency issues
    /// with configuration changing whilst queries are executing
    fn cloned(&self) -> Box<dyn ExtensionOptions>;

    /// Set the given `key`, `value` pair
    fn set(&mut self, key: &str, value: &str) -> Result<()>;

    /// Returns the [`ConfigEntry`] stored in this [`ExtensionOptions`]
    fn entries(&self) -> Vec<ConfigEntry>;
}

/// A type-safe container for [`ConfigExtension`]
#[derive(Debug, Default, Clone)]
pub struct Extensions(BTreeMap<&'static str, ExtensionBox>);

impl Extensions {
    /// Create a new, empty [`Extensions`]
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Registers a [`ConfigExtension`] with this [`ConfigOptions`]
    pub fn insert<T: ConfigExtension>(&mut self, extension: T) {
        assert_ne!(T::PREFIX, "datafusion");
        let e = ExtensionBox(Box::new(extension));
        self.0.insert(T::PREFIX, e);
    }

    /// Retrieves the extension of the given type if any
    pub fn get<T: ConfigExtension>(&self) -> Option<&T> {
        self.0.get(T::PREFIX)?.0.as_any().downcast_ref()
    }

    /// Retrieves the extension of the given type if any
    pub fn get_mut<T: ConfigExtension>(&mut self) -> Option<&mut T> {
        let e = self.0.get_mut(T::PREFIX)?;
        e.0.as_any_mut().downcast_mut()
    }
}

#[derive(Debug)]
struct ExtensionBox(Box<dyn ExtensionOptions>);

impl Clone for ExtensionBox {
    fn clone(&self) -> Self {
        Self(self.0.cloned())
    }
}

/// A trait implemented by `config_namespace` and for field types that provides
/// the ability to walk and mutate the configuration tree
pub trait ConfigField {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str);

    fn set(&mut self, key: &str, value: &str) -> Result<()>;
}

impl<F: ConfigField + Default> ConfigField for Option<F> {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        match self {
            Some(s) => s.visit(v, key, description),
            None => v.none(key, description),
        }
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        self.get_or_insert_with(Default::default).set(key, value)
    }
}

#[macro_export]
macro_rules! config_field {
    ($t:ty) => {
        impl ConfigField for $t {
            fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
                v.some(key, self, description)
            }

            fn set(&mut self, _: &str, value: &str) -> Result<()> {
                *self = value.parse().map_err(|e| {
                    DataFusionError::Context(
                        format!(concat!("Error parsing {} as ", stringify!($t),), value),
                        Box::new(DataFusionError::External(Box::new(e))),
                    )
                })?;
                Ok(())
            }
        }
    };
}

config_field!(String);
config_field!(bool);
config_field!(usize);
config_field!(f64);
config_field!(u64);

impl ConfigField for u8 {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        if value.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Input string for {} key is empty",
                key
            )));
        }
        // Check if the string is a valid number
        if let Ok(num) = value.parse::<u8>() {
            // TODO: Let's decide how we treat the numerical strings.
            *self = num;
        } else {
            let bytes = value.as_bytes();
            // Check if the first character is ASCII (single byte)
            if bytes.len() > 1 || !value.chars().next().unwrap().is_ascii() {
                return Err(DataFusionError::Configuration(format!(
                    "Error parsing {} as u8. Non-ASCII string provided",
                    value
                )));
            }
            *self = bytes[0];
        }
        Ok(())
    }
}

impl ConfigField for CompressionTypeVariant {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = CompressionTypeVariant::from_str(value)?;
        Ok(())
    }
}

/// An implementation trait used to recursively walk configuration
pub trait Visit {
    fn some<V: Display>(&mut self, key: &str, value: V, description: &'static str);

    fn none(&mut self, key: &str, description: &'static str);
}

/// Convenience macro to create [`ExtensionsOptions`].
///
/// The created structure implements the following traits:
///
/// - [`Clone`]
/// - [`Debug`]
/// - [`Default`]
/// - [`ExtensionOptions`]
///
/// # Usage
/// The syntax is:
///
/// ```text
/// extensions_options! {
///      /// Struct docs (optional).
///     [<vis>] struct <StructName> {
///         /// Field docs (optional)
///         [<vis>] <field_name>: <field_type>, default = <default_value>
///
///         ... more fields
///     }
/// }
/// ```
///
/// The placeholders are:
/// - `[<vis>]`: Optional visibility modifier like `pub` or `pub(crate)`.
/// - `<StructName>`: Struct name like `MyStruct`.
/// - `<field_name>`: Field name like `my_field`.
/// - `<field_type>`: Field type like `u8`.
/// - `<default_value>`: Default value matching the field type like `42`.
///
/// # Example
/// ```
/// use datafusion_common::extensions_options;
///
/// extensions_options! {
///     /// My own config options.
///     pub struct MyConfig {
///         /// Should "foo" be replaced by "bar"?
///         pub foo_to_bar: bool, default = true
///
///         /// How many "baz" should be created?
///         pub baz_count: usize, default = 1337
///     }
/// }
/// ```
///
///
/// [`Debug`]: std::fmt::Debug
/// [`ExtensionsOptions`]: crate::config::ExtensionOptions
#[macro_export]
macro_rules! extensions_options {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {
        $(#[doc = $struct_d])*
        #[derive(Debug, Clone)]
        #[non_exhaustive]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl $crate::config::ExtensionOptions for $struct_name {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn ::std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn $crate::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> $crate::Result<()> {
                match key {
                    $(
                       stringify!($field_name) => {
                        self.$field_name = value.parse().map_err(|e| {
                            $crate::DataFusionError::Context(
                                format!(concat!("Error parsing {} as ", stringify!($t),), value),
                                Box::new($crate::DataFusionError::External(Box::new(e))),
                            )
                        })?;
                        Ok(())
                       }
                    )*
                    _ => Err($crate::DataFusionError::Configuration(
                        format!(concat!("Config value \"{}\" not found on ", stringify!($struct_name)), key)
                    ))
                }
            }

            fn entries(&self) -> Vec<$crate::config::ConfigEntry> {
                vec![
                    $(
                        $crate::config::ConfigEntry {
                            key: stringify!($field_name).to_owned(),
                            value: (self.$field_name != $default).then(|| self.$field_name.to_string()),
                            description: concat!($($d),*).trim(),
                        },
                    )*
                ]
            }
        }
    }
}

/// These file types have special built in behavior for configuration.
/// Use TableOptions::Extensions for configuring other file types.
#[derive(Debug, Clone)]
pub enum ConfigFileType {
    CSV,
    #[cfg(feature = "parquet")]
    PARQUET,
    JSON,
}

/// Represents the configuration options available for handling different table formats within a data processing application.
/// This struct encompasses options for various file formats including CSV, Parquet, and JSON, allowing for flexible configuration
/// of parsing and writing behaviors specific to each format. Additionally, it supports extending functionality through custom extensions.
#[derive(Debug, Clone, Default)]
pub struct TableOptions {
    /// Configuration options for CSV file handling. This includes settings like the delimiter,
    /// quote character, and whether the first row is considered as headers.
    pub csv: CsvOptions,

    /// Configuration options for Parquet file handling. This includes settings for compression,
    /// encoding, and other Parquet-specific file characteristics.
    pub parquet: TableParquetOptions,

    /// Configuration options for JSON file handling.
    pub json: JsonOptions,

    /// The current file format that the table operations should assume. This option allows
    /// for dynamic switching between the supported file types (e.g., CSV, Parquet, JSON).
    pub current_format: Option<ConfigFileType>,

    /// Optional extensions that can be used to extend or customize the behavior of the table
    /// options. Extensions can be registered using `Extensions::insert` and might include
    /// custom file handling logic, additional configuration parameters, or other enhancements.
    pub extensions: Extensions,
}

impl ConfigField for TableOptions {
    /// Visits configuration settings for the current file format, or all formats if none is selected.
    ///
    /// This method adapts the behavior based on whether a file format is currently selected in `current_format`.
    /// If a format is selected, it visits only the settings relevant to that format. Otherwise,
    /// it visits all available format settings.
    fn visit<V: Visit>(&self, v: &mut V, _key_prefix: &str, _description: &'static str) {
        if let Some(file_type) = &self.current_format {
            match file_type {
                #[cfg(feature = "parquet")]
                ConfigFileType::PARQUET => self.parquet.visit(v, "format", ""),
                ConfigFileType::CSV => self.csv.visit(v, "format", ""),
                ConfigFileType::JSON => self.json.visit(v, "format", ""),
            }
        } else {
            self.csv.visit(v, "csv", "");
            self.parquet.visit(v, "parquet", "");
            self.json.visit(v, "json", "");
        }
    }

    /// Sets a configuration value for a specific key within `TableOptions`.
    ///
    /// This method delegates setting configuration values to the specific file format configurations,
    /// based on the current format selected. If no format is selected, it returns an error.
    ///
    /// # Parameters
    ///
    /// * `key`: The configuration key specifying which setting to adjust, prefixed with the format (e.g., "format.delimiter")
    /// for CSV format.
    /// * `value`: The value to set for the specified configuration key.
    ///
    /// # Returns
    ///
    /// A result indicating success or an error if the key is not recognized, if a format is not specified,
    /// or if setting the configuration value fails for the specific format.
    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Extensions are handled in the public `ConfigOptions::set`
        let (key, rem) = key.split_once('.').unwrap_or((key, ""));
        let Some(format) = &self.current_format else {
            return _config_err!("Specify a format for TableOptions");
        };
        match key {
            "format" => match format {
                #[cfg(feature = "parquet")]
                ConfigFileType::PARQUET => self.parquet.set(rem, value),
                ConfigFileType::CSV => self.csv.set(rem, value),
                ConfigFileType::JSON => self.json.set(rem, value),
            },
            _ => _config_err!("Config value \"{key}\" not found on TableOptions"),
        }
    }
}

impl TableOptions {
    /// Constructs a new instance of `TableOptions` with default settings.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with default configuration values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `TableOptions` instance initialized with settings from a given session config.
    ///
    /// # Parameters
    ///
    /// * `config`: A reference to the session `ConfigOptions` from which to derive initial settings.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with settings applied from the session config.
    pub fn default_from_session_config(config: &ConfigOptions) -> Self {
        let initial = TableOptions::default();
        initial.combine_with_session_config(config);
        initial
    }

    /// Updates the current `TableOptions` with settings from a given session config.
    ///
    /// # Parameters
    ///
    /// * `config`: A reference to the session `ConfigOptions` whose settings are to be applied.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with updated settings from the session config.
    pub fn combine_with_session_config(&self, config: &ConfigOptions) -> Self {
        let mut clone = self.clone();
        clone.parquet.global = config.execution.parquet.clone();
        clone
    }

    /// Sets the file format for the table.
    ///
    /// # Parameters
    ///
    /// * `format`: The file format to use (e.g., CSV, Parquet).
    pub fn set_config_format(&mut self, format: ConfigFileType) {
        self.current_format = Some(format);
    }

    /// Sets the extensions for this `TableOptions` instance.
    ///
    /// # Parameters
    ///
    /// * `extensions`: The `Extensions` instance to set.
    ///
    /// # Returns
    ///
    /// A new `TableOptions` instance with the specified extensions applied.
    pub fn with_extensions(mut self, extensions: Extensions) -> Self {
        self.extensions = extensions;
        self
    }

    /// Sets a specific configuration option.
    ///
    /// # Parameters
    ///
    /// * `key`: The configuration key (e.g., "format.delimiter").
    /// * `value`: The value to set for the specified key.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure in setting the configuration option.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let Some((prefix, _)) = key.split_once('.') else {
            return _config_err!("could not find config namespace for key \"{key}\"");
        };

        if prefix == "format" {
            return ConfigField::set(self, key, value);
        }

        if prefix == "execution" {
            return Ok(());
        }

        let Some(e) = self.extensions.0.get_mut(prefix) else {
            return _config_err!("Could not find config namespace \"{prefix}\"");
        };
        e.0.set(key, value)
    }

    /// Initializes a new `TableOptions` from a hash map of string settings.
    ///
    /// # Parameters
    ///
    /// * `settings`: A hash map where each key-value pair represents a configuration setting.
    ///
    /// # Returns
    ///
    /// A result containing the new `TableOptions` instance or an error if any setting could not be applied.
    pub fn from_string_hash_map(settings: &HashMap<String, String>) -> Result<Self> {
        let mut ret = Self::default();
        for (k, v) in settings {
            ret.set(k, v)?;
        }

        Ok(ret)
    }

    /// Modifies the current `TableOptions` instance with settings from a hash map.
    ///
    /// # Parameters
    ///
    /// * `settings`: A hash map where each key-value pair represents a configuration setting.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure in applying the settings.
    pub fn alter_with_string_hash_map(
        &mut self,
        settings: &HashMap<String, String>,
    ) -> Result<()> {
        for (k, v) in settings {
            self.set(k, v)?;
        }
        Ok(())
    }

    /// Retrieves all configuration entries from this `TableOptions`.
    ///
    /// # Returns
    ///
    /// A vector of `ConfigEntry` instances, representing all the configuration options within this `TableOptions`.
    pub fn entries(&self) -> Vec<ConfigEntry> {
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
        self.visit(&mut v, "format", "");

        v.0.extend(self.extensions.0.values().flat_map(|e| e.0.entries()));
        v.0
    }
}

/// Options that control how Parquet files are read, including global options
/// that apply to all columns and optional column-specific overrides
///
/// Closely tied to [`ParquetWriterOptions`](crate::file_options::parquet_writer::ParquetWriterOptions).
/// Properties not included in [`TableParquetOptions`] may not be configurable at the external API
/// (e.g. sorting_columns).
#[derive(Clone, Default, Debug, PartialEq)]
pub struct TableParquetOptions {
    /// Global Parquet options that propagates to all columns.
    pub global: ParquetOptions,
    /// Column specific options. Default usage is parquet.XX::column.
    pub column_specific_options: HashMap<String, ColumnOptions>,
    /// Additional file-level metadata to include. Inserted into the key_value_metadata
    /// for the written [`FileMetaData`](https://docs.rs/parquet/latest/parquet/file/metadata/struct.FileMetaData.html).
    ///
    /// Multiple entries are permitted
    /// ```sql
    /// OPTIONS (
    ///    'format.metadata::key1' '',
    ///    'format.metadata::key2' 'value',
    ///    'format.metadata::key3' 'value has spaces',
    ///    'format.metadata::key4' 'value has special chars :: :',
    ///    'format.metadata::key_dupe' 'original will be overwritten',
    ///    'format.metadata::key_dupe' 'final'
    /// )
    /// ```
    pub key_value_metadata: HashMap<String, Option<String>>,
}

impl TableParquetOptions {
    /// Return new default TableParquetOptions
    pub fn new() -> Self {
        Self::default()
    }
}

impl ConfigField for TableParquetOptions {
    fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, description: &'static str) {
        self.global.visit(v, key_prefix, description);
        self.column_specific_options
            .visit(v, key_prefix, description)
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        // Determine if the key is a global, metadata, or column-specific setting
        if key.starts_with("metadata::") {
            let k = match key.split("::").collect::<Vec<_>>()[..] {
                [_meta] | [_meta, ""] => {
                    return _config_err!(
                        "Invalid metadata key provided, missing key in metadata::<key>"
                    )
                }
                [_meta, k] => k.into(),
                _ => {
                    return _config_err!(
                        "Invalid metadata key provided, found too many '::' in \"{key}\""
                    )
                }
            };
            self.key_value_metadata.insert(k, Some(value.into()));
            Ok(())
        } else if key.contains("::") {
            self.column_specific_options.set(key, value)
        } else {
            self.global.set(key, value)
        }
    }
}

macro_rules! config_namespace_with_hashmap {
    (
     $(#[doc = $struct_d:tt])*
     $vis:vis struct $struct_name:ident {
        $(
        $(#[doc = $d:tt])*
        $field_vis:vis $field_name:ident : $field_type:ty, default = $default:expr
        )*$(,)*
    }
    ) => {

        $(#[doc = $struct_d])*
        #[derive(Debug, Clone, PartialEq)]
        $vis struct $struct_name{
            $(
            $(#[doc = $d])*
            $field_vis $field_name : $field_type,
            )*
        }

        impl ConfigField for $struct_name {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let (key, rem) = key.split_once('.').unwrap_or((key, ""));
                match key {
                    $(
                       stringify!($field_name) => self.$field_name.set(rem, value),
                    )*
                    _ => _config_err!(
                        "Config value \"{}\" not found on {}", key, stringify!($struct_name)
                    )
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                $(
                let key = format!(concat!("{}.", stringify!($field_name)), key_prefix);
                let desc = concat!($($d),*).trim();
                self.$field_name.visit(v, key.as_str(), desc);
                )*
            }
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: $default),*
                }
            }
        }

        impl ConfigField for HashMap<String,$struct_name> {
            fn set(&mut self, key: &str, value: &str) -> Result<()> {
                let parts: Vec<&str> = key.splitn(2, "::").collect();
                match parts.as_slice() {
                    [inner_key, hashmap_key] => {
                        // Get or create the ColumnOptions for the specified column
                        let inner_value = self
                            .entry((*hashmap_key).to_owned())
                            .or_insert_with($struct_name::default);

                        inner_value.set(inner_key, value)
                    }
                    _ => _config_err!("Unrecognized key '{key}'."),
                }
            }

            fn visit<V: Visit>(&self, v: &mut V, key_prefix: &str, _description: &'static str) {
                for (column_name, col_options) in self {
                    $(
                    let key = format!("{}.{field}::{}", key_prefix, column_name, field = stringify!($field_name));
                    let desc = concat!($($d),*).trim();
                    col_options.$field_name.visit(v, key.as_str(), desc);
                    )*
                }
            }
        }
    }
}

config_namespace_with_hashmap! {
    pub struct ColumnOptions {
        /// Sets if bloom filter is enabled for the column path.
        pub bloom_filter_enabled: Option<bool>, default = None

        /// Sets encoding for the column path.
        /// Valid values are: plain, plain_dictionary, rle,
        /// bit_packed, delta_binary_packed, delta_length_byte_array,
        /// delta_byte_array, rle_dictionary, and byte_stream_split.
        /// These values are not case-sensitive. If NULL, uses
        /// default parquet options
        pub encoding: Option<String>, default = None

        /// Sets if dictionary encoding is enabled for the column path. If NULL, uses
        /// default parquet options
        pub dictionary_enabled: Option<bool>, default = None

        /// Sets default parquet compression codec for the column path.
        /// Valid values are: uncompressed, snappy, gzip(level),
        /// lzo, brotli(level), lz4, zstd(level), and lz4_raw.
        /// These values are not case-sensitive. If NULL, uses
        /// default parquet options
        pub compression: Option<String>, default = None

        /// Sets if statistics are enabled for the column
        /// Valid values are: "none", "chunk", and "page"
        /// These values are not case sensitive. If NULL, uses
        /// default parquet options
        pub statistics_enabled: Option<String>, default = None

        /// Sets bloom filter false positive probability for the column path. If NULL, uses
        /// default parquet options
        pub bloom_filter_fpp: Option<f64>, default = None

        /// Sets bloom filter number of distinct values. If NULL, uses
        /// default parquet options
        pub bloom_filter_ndv: Option<u64>, default = None

        /// Sets max statistics size for the column path. If NULL, uses
        /// default parquet options
        pub max_statistics_size: Option<usize>, default = None
    }
}

config_namespace! {
    /// Options controlling CSV format
    pub struct CsvOptions {
        /// Specifies whether there is a CSV header (i.e. the first line
        /// consists of is column names). The value `None` indicates that
        /// the configuration should be consulted.
        pub has_header: Option<bool>, default = None
        pub delimiter: u8, default = b','
        pub quote: u8, default = b'"'
        pub escape: Option<u8>, default = None
        pub double_quote: Option<bool>, default = None
        pub compression: CompressionTypeVariant, default = CompressionTypeVariant::UNCOMPRESSED
        pub schema_infer_max_rec: usize, default = 100
        pub date_format: Option<String>, default = None
        pub datetime_format: Option<String>, default = None
        pub timestamp_format: Option<String>, default = None
        pub timestamp_tz_format: Option<String>, default = None
        pub time_format: Option<String>, default = None
        pub null_value: Option<String>, default = None
        pub comment: Option<u8>, default = None
    }
}

impl CsvOptions {
    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_compression(
        mut self,
        compression_type_variant: CompressionTypeVariant,
    ) -> Self {
        self.compression = compression_type_variant;
        self
    }

    /// Set a limit in terms of records to scan to infer the schema
    /// - default to `DEFAULT_SCHEMA_INFER_MAX_RECORD`
    pub fn with_schema_infer_max_rec(mut self, max_rec: usize) -> Self {
        self.schema_infer_max_rec = max_rec;
        self
    }

    /// Set true to indicate that the first line is a header.
    /// - default to true
    pub fn with_has_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// Returns true if the first line is a header. If format options does not
    /// specify whether there is a header, returns `None` (indicating that the
    /// configuration should be consulted).
    pub fn has_header(&self) -> Option<bool> {
        self.has_header
    }

    /// The character separating values within a row.
    /// - default to ','
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    /// The quote character in a row.
    /// - default to '"'
    pub fn with_quote(mut self, quote: u8) -> Self {
        self.quote = quote;
        self
    }

    /// The escape character in a row.
    /// - default is None
    pub fn with_escape(mut self, escape: Option<u8>) -> Self {
        self.escape = escape;
        self
    }

    /// Set true to indicate that the CSV quotes should be doubled.
    /// - default to true
    pub fn with_double_quote(mut self, double_quote: bool) -> Self {
        self.double_quote = Some(double_quote);
        self
    }

    /// Set a `CompressionTypeVariant` of CSV
    /// - defaults to `CompressionTypeVariant::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        compression: CompressionTypeVariant,
    ) -> Self {
        self.compression = compression;
        self
    }

    /// The delimiter character.
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }

    /// The quote character.
    pub fn quote(&self) -> u8 {
        self.quote
    }

    /// The escape character.
    pub fn escape(&self) -> Option<u8> {
        self.escape
    }
}

config_namespace! {
    /// Options controlling JSON format
    pub struct JsonOptions {
        pub compression: CompressionTypeVariant, default = CompressionTypeVariant::UNCOMPRESSED
        pub schema_infer_max_rec: usize, default = 100
    }
}

pub trait FormatOptionsExt: Display {}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum FormatOptions {
    CSV(CsvOptions),
    JSON(JsonOptions),
    #[cfg(feature = "parquet")]
    PARQUET(TableParquetOptions),
    AVRO,
    ARROW,
}

impl Display for FormatOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let out = match self {
            FormatOptions::CSV(_) => "csv",
            FormatOptions::JSON(_) => "json",
            #[cfg(feature = "parquet")]
            FormatOptions::PARQUET(_) => "parquet",
            FormatOptions::AVRO => "avro",
            FormatOptions::ARROW => "arrow",
        };
        write!(f, "{}", out)
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;

    use crate::config::{
        ConfigEntry, ConfigExtension, ConfigFileType, ExtensionOptions, Extensions,
        TableOptions,
    };

    #[derive(Default, Debug, Clone)]
    pub struct TestExtensionConfig {
        /// Should "foo" be replaced by "bar"?
        pub properties: HashMap<String, String>,
    }

    impl ExtensionOptions for TestExtensionConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }

        fn cloned(&self) -> Box<dyn ExtensionOptions> {
            Box::new(self.clone())
        }

        fn set(&mut self, key: &str, value: &str) -> crate::Result<()> {
            let (key, rem) = key.split_once('.').unwrap_or((key, ""));
            assert_eq!(key, "test");
            self.properties.insert(rem.to_owned(), value.to_owned());
            Ok(())
        }

        fn entries(&self) -> Vec<ConfigEntry> {
            self.properties
                .iter()
                .map(|(k, v)| ConfigEntry {
                    key: k.into(),
                    value: Some(v.into()),
                    description: "",
                })
                .collect()
        }
    }

    impl ConfigExtension for TestExtensionConfig {
        const PREFIX: &'static str = "test";
    }

    #[test]
    fn create_table_config() {
        let mut extension = Extensions::new();
        extension.insert(TestExtensionConfig::default());
        let table_config = TableOptions::new().with_extensions(extension);
        let kafka_config = table_config.extensions.get::<TestExtensionConfig>();
        assert!(kafka_config.is_some())
    }

    #[test]
    fn alter_test_extension_config() {
        let mut extension = Extensions::new();
        extension.insert(TestExtensionConfig::default());
        let mut table_config = TableOptions::new().with_extensions(extension);
        table_config.set_config_format(ConfigFileType::CSV);
        table_config.set("format.delimiter", ";").unwrap();
        assert_eq!(table_config.csv.delimiter, b';');
        table_config.set("test.bootstrap.servers", "asd").unwrap();
        let kafka_config = table_config
            .extensions
            .get::<TestExtensionConfig>()
            .unwrap();
        assert_eq!(
            kafka_config.properties.get("bootstrap.servers").unwrap(),
            "asd"
        );
    }

    #[test]
    fn csv_u8_table_options() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::CSV);
        table_config.set("format.delimiter", ";").unwrap();
        assert_eq!(table_config.csv.delimiter as char, ';');
        table_config.set("format.escape", "\"").unwrap();
        assert_eq!(table_config.csv.escape.unwrap() as char, '"');
        table_config.set("format.escape", "\'").unwrap();
        assert_eq!(table_config.csv.escape.unwrap() as char, '\'');
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .set("format.bloom_filter_enabled::col1", "true")
            .unwrap();
        assert_eq!(
            table_config.parquet.column_specific_options["col1"].bloom_filter_enabled,
            Some(true)
        );
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options_config_entry() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config
            .set("format.bloom_filter_enabled::col1", "true")
            .unwrap();
        let entries = table_config.entries();
        assert!(entries
            .iter()
            .any(|item| item.key == "format.bloom_filter_enabled::col1"))
    }

    #[cfg(feature = "parquet")]
    #[test]
    fn parquet_table_options_config_metadata_entry() {
        let mut table_config = TableOptions::new();
        table_config.set_config_format(ConfigFileType::PARQUET);
        table_config.set("format.metadata::key1", "").unwrap();
        table_config.set("format.metadata::key2", "value2").unwrap();
        table_config
            .set("format.metadata::key3", "value with spaces ")
            .unwrap();
        table_config
            .set("format.metadata::key4", "value with special chars :: :")
            .unwrap();

        let parsed_metadata = table_config.parquet.key_value_metadata.clone();
        assert_eq!(parsed_metadata.get("should not exist1"), None);
        assert_eq!(parsed_metadata.get("key1"), Some(&Some("".into())));
        assert_eq!(parsed_metadata.get("key2"), Some(&Some("value2".into())));
        assert_eq!(
            parsed_metadata.get("key3"),
            Some(&Some("value with spaces ".into()))
        );
        assert_eq!(
            parsed_metadata.get("key4"),
            Some(&Some("value with special chars :: :".into()))
        );

        // duplicate keys are overwritten
        table_config.set("format.metadata::key_dupe", "A").unwrap();
        table_config.set("format.metadata::key_dupe", "B").unwrap();
        let parsed_metadata = table_config.parquet.key_value_metadata;
        assert_eq!(parsed_metadata.get("key_dupe"), Some(&Some("B".into())));
    }
}
