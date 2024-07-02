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

use std::collections::HashMap;
use std::env;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionConfig;
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use datafusion_cli::catalog::DynamicFileCatalog;
use datafusion_cli::functions::ParquetMetadataFunc;
use datafusion_cli::{
    exec,
    pool_type::PoolType,
    print_format::PrintFormat,
    print_options::{MaxRows, PrintOptions},
    DATAFUSION_CLI_VERSION,
};

use clap::Parser;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'b',
        long,
        help = "The batch size of each query, or use DataFusion default",
        validator(is_valid_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(
        short = 'c',
        long,
        multiple_values = true,
        help = "Execute the given command string(s), then exit. Commands are expected to be non empty.",
        validator(is_valid_command)
    )]
    command: Vec<String>,

    #[clap(
        short = 'm',
        long,
        help = "The memory pool limitation (e.g. '10g'), default to None (no limit)",
        validator(is_valid_memory_pool_size)
    )]
    memory_limit: Option<String>,

    #[clap(
        short,
        long,
        multiple_values = true,
        help = "Execute commands from file(s), then exit",
        validator(is_valid_file)
    )]
    file: Vec<String>,

    #[clap(
        short = 'r',
        long,
        multiple_values = true,
        help = "Run the provided files on startup instead of ~/.datafusionrc",
        validator(is_valid_file),
        conflicts_with = "file"
    )]
    rc: Option<Vec<String>>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Automatic)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,

    #[clap(
        long,
        help = "Specify the memory pool type 'greedy' or 'fair'",
        default_value_t = PoolType::Greedy
    )]
    mem_pool_type: PoolType,

    #[clap(
        long,
        help = "The max number of rows to display for 'Table' format\n[possible values: numbers(0/10/...), inf(no limit)]",
        default_value = "40"
    )]
    maxrows: MaxRows,

    #[clap(long, help = "Enables console syntax highlighting")]
    color: bool,
}

#[tokio::main]
/// Calls [`main_inner`], then handles printing errors and returning the correct exit code
pub async fn main() -> ExitCode {
    if let Err(e) = main_inner().await {
        println!("Error: {e}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

/// Main CLI entrypoint
async fn main_inner() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("DataFusion CLI v{}", DATAFUSION_CLI_VERSION);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

    if let Some(batch_size) = args.batch_size {
        session_config = session_config.with_batch_size(batch_size);
    };

    let rt_config = RuntimeConfig::new();
    let rt_config =
        // set memory pool size
        if let Some(memory_limit) = args.memory_limit {
            // unwrap is safe here because is_valid_memory_pool_size already checked the value
            let memory_limit = extract_memory_pool_size(&memory_limit).unwrap();
            // set memory pool type
            match args.mem_pool_type {
                PoolType::Fair => rt_config
                    .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit))),
                PoolType::Greedy => rt_config
                    .with_memory_pool(Arc::new(GreedyMemoryPool::new(memory_limit)))
            }
        } else {
            rt_config
        };

    let runtime_env = create_runtime_env(rt_config.clone())?;

    let mut ctx =
        SessionContext::new_with_config_rt(session_config.clone(), Arc::new(runtime_env));
    ctx.refresh_catalogs().await?;
    // enable dynamic file query
    ctx.enable_url_table()?;
    // install dynamic catalog provider that can register required object stores
    ctx.register_catalog_list(Arc::new(DynamicFileCatalog::new(
        ctx.state().catalog_list().clone(),
        ctx.state_weak_ref(),
    )));
    // register `parquet_metadata` table function to get metadata from parquet files
    ctx.register_udtf("parquet_metadata", Arc::new(ParquetMetadataFunc {}));

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
        maxrows: args.maxrows,
        color: args.color,
    };

    let commands = args.command;
    let files = args.file;
    let rc = match args.rc {
        Some(file) => file,
        None => {
            let mut files = Vec::new();
            let home = dirs::home_dir();
            if let Some(p) = home {
                let home_rc = p.join(".datafusionrc");
                if home_rc.exists() {
                    files.push(home_rc.into_os_string().into_string().unwrap());
                }
            }
            files
        }
    };

    if commands.is_empty() && files.is_empty() {
        if !rc.is_empty() {
            exec::exec_from_files(&mut ctx, rc, &print_options).await?;
        }
        // TODO maybe we can have thiserror for cli but for now let's keep it simple
        return exec::exec_from_repl(&mut ctx, &mut print_options)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)));
    }

    if !files.is_empty() {
        exec::exec_from_files(&mut ctx, files, &print_options).await?;
    }

    if !commands.is_empty() {
        exec::exec_from_commands(&mut ctx, commands, &print_options).await?;
    }

    Ok(())
}

fn create_runtime_env(rn_config: RuntimeConfig) -> Result<RuntimeEnv> {
    RuntimeEnv::new(rn_config)
}

fn is_valid_file(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}

fn is_valid_memory_pool_size(size: &str) -> Result<(), String> {
    match extract_memory_pool_size(size) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}

fn is_valid_command(command: &str) -> Result<(), String> {
    if !command.is_empty() {
        Ok(())
    } else {
        Err("-c flag expects only non empty commands".to_string())
    }
}

#[derive(Debug, Clone, Copy)]
enum ByteUnit {
    Byte,
    KiB,
    MiB,
    GiB,
    TiB,
}

impl ByteUnit {
    fn multiplier(&self) -> u64 {
        match self {
            ByteUnit::Byte => 1,
            ByteUnit::KiB => 1 << 10,
            ByteUnit::MiB => 1 << 20,
            ByteUnit::GiB => 1 << 30,
            ByteUnit::TiB => 1 << 40,
        }
    }
}

fn extract_memory_pool_size(size: &str) -> Result<usize, String> {
    fn byte_suffixes() -> &'static HashMap<&'static str, ByteUnit> {
        static BYTE_SUFFIXES: OnceLock<HashMap<&'static str, ByteUnit>> = OnceLock::new();
        BYTE_SUFFIXES.get_or_init(|| {
            let mut m = HashMap::new();
            m.insert("b", ByteUnit::Byte);
            m.insert("k", ByteUnit::KiB);
            m.insert("kb", ByteUnit::KiB);
            m.insert("m", ByteUnit::MiB);
            m.insert("mb", ByteUnit::MiB);
            m.insert("g", ByteUnit::GiB);
            m.insert("gb", ByteUnit::GiB);
            m.insert("t", ByteUnit::TiB);
            m.insert("tb", ByteUnit::TiB);
            m
        })
    }

    fn suffix_re() -> &'static regex::Regex {
        static SUFFIX_REGEX: OnceLock<regex::Regex> = OnceLock::new();
        SUFFIX_REGEX.get_or_init(|| regex::Regex::new(r"^(-?[0-9]+)([a-z]+)?$").unwrap())
    }

    let lower = size.to_lowercase();
    if let Some(caps) = suffix_re().captures(&lower) {
        let num_str = caps.get(1).unwrap().as_str();
        let num = num_str.parse::<usize>().map_err(|_| {
            format!("Invalid numeric value in memory pool size '{}'", size)
        })?;

        let suffix = caps.get(2).map(|m| m.as_str()).unwrap_or("b");
        let unit = byte_suffixes()
            .get(suffix)
            .ok_or_else(|| format!("Invalid memory pool size '{}'", size))?;
        let memory_pool_size = usize::try_from(unit.multiplier())
            .ok()
            .and_then(|multiplier| num.checked_mul(multiplier))
            .ok_or_else(|| format!("Memory pool size '{}' is too large", size))?;

        Ok(memory_pool_size)
    } else {
        Err(format!("Invalid memory pool size '{}'", size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

    fn assert_conversion(input: &str, expected: Result<usize, String>) {
        let result = extract_memory_pool_size(input);
        match expected {
            Ok(v) => assert_eq!(result.unwrap(), v),
            Err(e) => assert_eq!(result.unwrap_err(), e),
        }
    }

    #[test]
    fn memory_pool_size() -> Result<(), String> {
        // Test basic sizes without suffix, assumed to be bytes
        assert_conversion("5", Ok(5));
        assert_conversion("100", Ok(100));

        // Test various units
        assert_conversion("5b", Ok(5));
        assert_conversion("4k", Ok(4 * 1024));
        assert_conversion("4kb", Ok(4 * 1024));
        assert_conversion("20m", Ok(20 * 1024 * 1024));
        assert_conversion("20mb", Ok(20 * 1024 * 1024));
        assert_conversion("2g", Ok(2 * 1024 * 1024 * 1024));
        assert_conversion("2gb", Ok(2 * 1024 * 1024 * 1024));
        assert_conversion("3t", Ok(3 * 1024 * 1024 * 1024 * 1024));
        assert_conversion("4tb", Ok(4 * 1024 * 1024 * 1024 * 1024));

        // Test case insensitivity
        assert_conversion("4K", Ok(4 * 1024));
        assert_conversion("4KB", Ok(4 * 1024));
        assert_conversion("20M", Ok(20 * 1024 * 1024));
        assert_conversion("20MB", Ok(20 * 1024 * 1024));
        assert_conversion("2G", Ok(2 * 1024 * 1024 * 1024));
        assert_conversion("2GB", Ok(2 * 1024 * 1024 * 1024));
        assert_conversion("2T", Ok(2 * 1024 * 1024 * 1024 * 1024));

        // Test invalid input
        assert_conversion(
            "invalid",
            Err("Invalid memory pool size 'invalid'".to_string()),
        );
        assert_conversion("4kbx", Err("Invalid memory pool size '4kbx'".to_string()));
        assert_conversion(
            "-20mb",
            Err("Invalid numeric value in memory pool size '-20mb'".to_string()),
        );
        assert_conversion(
            "-100",
            Err("Invalid numeric value in memory pool size '-100'".to_string()),
        );
        assert_conversion(
            "12k12k",
            Err("Invalid memory pool size '12k12k'".to_string()),
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_metadata_works() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_metadata", Arc::new(ParquetMetadataFunc {}));

        // input with single quote
        let sql =
            "SELECT * FROM parquet_metadata('../datafusion/core/tests/data/fixed_size_list_array.parquet')";
        let df = ctx.sql(sql).await?;
        let rbs = df.collect().await?;

        let excepted = [
            "+-------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+-------+-----------+-----------+------------------+----------------------+-----------------+-----------------+-------------+------------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+",
            "| filename                                                    | row_group_id | row_group_num_rows | row_group_num_columns | row_group_bytes | column_id | file_offset | num_values | path_in_schema | type  | stats_min | stats_max | stats_null_count | stats_distinct_count | stats_min_value | stats_max_value | compression | encodings                    | index_page_offset | dictionary_page_offset | data_page_offset | total_compressed_size | total_uncompressed_size |",
            "+-------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+-------+-----------+-----------+------------------+----------------------+-----------------+-----------------+-------------+------------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+",
            "| ../datafusion/core/tests/data/fixed_size_list_array.parquet | 0            | 2                  | 1                     | 123             | 0         | 125         | 4          | \"f0.list.item\" | INT64 | 1         | 4         | 0                |                      | 1               | 4               | SNAPPY      | [RLE_DICTIONARY, PLAIN, RLE] |                   | 4                      | 46               | 121                   | 123                     |",
            "+-------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+-------+-----------+-----------+------------------+----------------------+-----------------+-----------------+-------------+------------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+",
        ];
        assert_batches_eq!(excepted, &rbs);

        // input with double quote
        let sql =
            "SELECT * FROM parquet_metadata(\"../datafusion/core/tests/data/fixed_size_list_array.parquet\")";
        let df = ctx.sql(sql).await?;
        let rbs = df.collect().await?;
        assert_batches_eq!(excepted, &rbs);

        Ok(())
    }

    #[tokio::test]
    async fn test_parquet_metadata_works_with_strings() -> Result<(), DataFusionError> {
        let ctx = SessionContext::new();
        ctx.register_udtf("parquet_metadata", Arc::new(ParquetMetadataFunc {}));

        // input with string columns
        let sql =
            "SELECT * FROM parquet_metadata('../parquet-testing/data/data_index_bloom_encoding_stats.parquet')";
        let df = ctx.sql(sql).await?;
        let rbs = df.collect().await?;

        let excepted = [

"+-----------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+------------+-----------+-----------+------------------+----------------------+-----------------+-----------------+--------------------+--------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+",
"| filename                                                        | row_group_id | row_group_num_rows | row_group_num_columns | row_group_bytes | column_id | file_offset | num_values | path_in_schema | type       | stats_min | stats_max | stats_null_count | stats_distinct_count | stats_min_value | stats_max_value | compression        | encodings                | index_page_offset | dictionary_page_offset | data_page_offset | total_compressed_size | total_uncompressed_size |",
"+-----------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+------------+-----------+-----------+------------------+----------------------+-----------------+-----------------+--------------------+--------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+",
"| ../parquet-testing/data/data_index_bloom_encoding_stats.parquet | 0            | 14                 | 1                     | 163             | 0         | 4           | 14         | \"String\"       | BYTE_ARRAY | Hello     | today     | 0                |                      | Hello           | today           | GZIP(GzipLevel(6)) | [BIT_PACKED, RLE, PLAIN] |                   |                        | 4                | 152                   | 163                     |",
"+-----------------------------------------------------------------+--------------+--------------------+-----------------------+-----------------+-----------+-------------+------------+----------------+------------+-----------+-----------+------------------+----------------------+-----------------+-----------------+--------------------+--------------------------+-------------------+------------------------+------------------+-----------------------+-------------------------+"
        ];
        assert_batches_eq!(excepted, &rbs);

        Ok(())
    }
}
