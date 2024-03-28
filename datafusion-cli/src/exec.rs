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

//! Execution functions

use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use crate::helper::split_from_semicolon;
use crate::print_format::PrintFormat;
use crate::{
    command::{Command, OutputFormat},
    helper::{unescape_input, CliHelper},
    object_storage::{get_object_store, register_options},
    print_options::{MaxRows, PrintOptions},
};

use datafusion::common::instant::Instant;
use datafusion::common::plan_datafusion_err;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::physical_plan::{collect, execute_stream, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::dialect::dialect_from_str;

use datafusion_common::FileType;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use tokio::signal;

/// run and execute SQL statements and commands, against a context with the given print options
pub async fn exec_from_commands(
    ctx: &mut SessionContext,
    commands: Vec<String>,
    print_options: &PrintOptions,
) -> Result<()> {
    for sql in commands {
        exec_and_print(ctx, print_options, sql).await?;
    }

    Ok(())
}

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
    ctx: &mut SessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
) -> Result<()> {
    let mut query = "".to_owned();

    for line in reader.lines() {
        match line {
            Ok(line) if line.starts_with("--") => {
                continue;
            }
            Ok(line) => {
                let line = line.trim_end();
                query.push_str(line);
                if line.ends_with(';') {
                    match exec_and_print(ctx, print_options, query).await {
                        Ok(_) => {}
                        Err(err) => eprintln!("{err}"),
                    }
                    query = "".to_owned();
                } else {
                    query.push('\n');
                }
            }
            _ => {
                break;
            }
        }
    }

    // run the left over query if the last statement doesn't contain ‘;’
    // ignore if it only consists of '\n'
    if query.contains(|c| c != '\n') {
        exec_and_print(ctx, print_options, query).await?;
    }

    Ok(())
}

pub async fn exec_from_files(
    ctx: &mut SessionContext,
    files: Vec<String>,
    print_options: &PrintOptions,
) -> Result<()> {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();

    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await?;
    }

    Ok(())
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(
    ctx: &mut SessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(CliHelper::new(
        &ctx.task_ctx().session_config().options().sql_parser.dialect,
        print_options.color,
    )));
    rl.load_history(".history").ok();

    loop {
        match rl.readline("❯ ") {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end())?;
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) = command.execute(print_options).await {
                                        eprintln!("{e}")
                                    }
                                } else {
                                    eprintln!(
                                        "'\\{}' is not a valid command",
                                        &line[1..]
                                    );
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        }
                        _ => {
                            if let Err(e) = cmd.execute(ctx, print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }
            Ok(line) => {
                let lines = split_from_semicolon(line);
                for line in lines {
                    rl.add_history_entry(line.trim_end())?;
                    tokio::select! {
                        res = exec_and_print(ctx, print_options, line) => match res {
                            Ok(_) => {}
                            Err(err) => eprintln!("{err}"),
                        },
                        _ = signal::ctrl_c() => {
                            println!("^C");
                            continue
                        },
                    }
                    // dialect might have changed
                    rl.helper_mut().unwrap().set_dialect(
                        &ctx.task_ctx().session_config().options().sql_parser.dialect,
                    );
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            }
            Err(err) => {
                eprintln!("Unknown error happened {:?}", err);
                break;
            }
        }
    }

    rl.save_history(".history")
}

async fn exec_and_print(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();
    let sql = unescape_input(&sql)?;
    let task_ctx = ctx.task_ctx();
    let dialect = &task_ctx.session_config().options().sql_parser.dialect;
    let dialect = dialect_from_str(dialect).ok_or_else(|| {
        plan_datafusion_err!(
            "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi."
        )
    })?;

    let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
    for statement in statements {
        let plan = create_plan(ctx, statement).await?;

        // For plans like `Explain` ignore `MaxRows` option and always display all rows
        let should_ignore_maxrows = matches!(
            plan,
            LogicalPlan::Explain(_)
                | LogicalPlan::DescribeTable(_)
                | LogicalPlan::Analyze(_)
        );
        let df = ctx.execute_logical_plan(plan).await?;
        let physical_plan = df.create_physical_plan().await?;

        if physical_plan.execution_mode().is_unbounded() {
            let stream = execute_stream(physical_plan, task_ctx.clone())?;
            print_options.print_stream(stream, now).await?;
        } else {
            let mut print_options = print_options.clone();
            if should_ignore_maxrows {
                print_options.maxrows = MaxRows::Unlimited;
            }
            if print_options.format == PrintFormat::Automatic {
                print_options.format = PrintFormat::Table;
            }
            let results = collect(physical_plan, task_ctx.clone()).await?;
            print_options.print_batches(&results, now)?;
        }
    }

    Ok(())
}

async fn create_plan(
    ctx: &mut SessionContext,
    statement: Statement,
) -> Result<LogicalPlan, DataFusionError> {
    let mut plan = ctx.state().statement_to_plan(statement).await?;

    // Note that cmd is a mutable reference so that create_external_table function can remove all
    // datafusion-cli specific options before passing through to datafusion. Otherwise, datafusion
    // will raise Configuration errors.
    if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
        register_object_store_and_config_extensions(
            ctx,
            &cmd.location,
            &cmd.options,
            None,
        )
        .await?;
    }

    if let LogicalPlan::Copy(copy_to) = &mut plan {
        let format: FileType = (&copy_to.format_options).into();

        register_object_store_and_config_extensions(
            ctx,
            &copy_to.output_url,
            &copy_to.options,
            Some(format),
        )
        .await?;
    }
    Ok(plan)
}

/// Asynchronously registers an object store and its configuration extensions
/// to the session context.
///
/// This function dynamically registers a cloud object store based on the given
/// location and options. It first parses the location to determine the scheme
/// and constructs the URL accordingly. Depending on the scheme, it also registers
/// relevant options. The function then alters the default table options with the
/// given custom options. Finally, it retrieves and registers the object store
/// in the session context.
///
/// # Parameters
///
/// * `ctx`: A reference to the `SessionContext` for registering the object store.
/// * `location`: A string reference representing the location of the object store.
/// * `options`: A reference to a hash map containing configuration options for
///   the object store.
///
/// # Returns
///
/// A `Result<()>` which is an Ok value indicating successful registration, or
/// an error upon failure.
///
/// # Errors
///
/// This function can return an error if the location parsing fails, options
/// alteration fails, or if the object store cannot be retrieved and registered
/// successfully.
pub(crate) async fn register_object_store_and_config_extensions(
    ctx: &SessionContext,
    location: &String,
    options: &HashMap<String, String>,
    format: Option<FileType>,
) -> Result<()> {
    // Parse the location URL to extract the scheme and other components
    let table_path = ListingTableUrl::parse(location)?;

    // Extract the scheme (e.g., "s3", "gcs") from the parsed URL
    let scheme = table_path.scheme();

    // Obtain a reference to the URL
    let url = table_path.as_ref();

    // Register the options based on the scheme extracted from the location
    register_options(ctx, scheme);

    // Clone and modify the default table options based on the provided options
    let mut table_options = ctx.state().default_table_options().clone();
    if let Some(format) = format {
        table_options.set_file_format(format);
    }
    table_options.alter_with_string_hash_map(options)?;

    // Retrieve the appropriate object store based on the scheme, URL, and modified table options
    let store = get_object_store(&ctx.state(), scheme, url, &table_options).await?;

    // Register the retrieved object store in the session context's runtime environment
    ctx.runtime_env().register_object_store(url, store);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion_common::config::FormatOptions;
    use datafusion_common::plan_err;

    use url::Url;

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            register_object_store_and_config_extensions(
                &ctx,
                &cmd.location,
                &cmd.options,
                None,
            )
            .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    async fn copy_to_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        // AWS CONFIG register.

        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Copy(cmd) = &plan {
            let format: FileType = (&cmd.format_options).into();
            register_object_store_and_config_extensions(
                &ctx,
                &cmd.output_url,
                &cmd.options,
                Some(format),
            )
            .await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        // Ensure the URL is supported by the object store
        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_http() -> Result<()> {
        // Should be OK
        let location = "http://example.com/file.parquet";
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }
    #[tokio::test]
    async fn copy_to_external_object_store_test() -> Result<()> {
        let locations = vec![
            "s3://bucket/path/file.parquet",
            "oss://bucket/path/file.parquet",
            "cos://bucket/path/file.parquet",
            "gcs://bucket/path/file.parquet",
        ];
        let mut ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let dialect = &task_ctx.session_config().options().sql_parser.dialect;
        let dialect = dialect_from_str(dialect).ok_or_else(|| {
            plan_datafusion_err!(
                "Unsupported SQL dialect: {dialect}. Available dialects: \
                 Generic, MySQL, PostgreSQL, Hive, SQLite, Snowflake, Redshift, \
                 MsSQL, ClickHouse, BigQuery, Ansi."
            )
        })?;
        for location in locations {
            let sql = format!("copy (values (1,2)) to '{}' STORED AS PARQUET;", location);
            let statements = DFParser::parse_sql_with_dialect(&sql, dialect.as_ref())?;
            for statement in statements {
                //Should not fail
                let mut plan = create_plan(&mut ctx, statement).await?;
                if let LogicalPlan::Copy(copy_to) = &mut plan {
                    assert_eq!(copy_to.output_url, location);
                    assert!(matches!(copy_to.format_options, FormatOptions::PARQUET(_)));
                    ctx.runtime_env()
                        .object_store_registry
                        .get_store(&Url::parse(&copy_to.output_url).unwrap())?;
                } else {
                    return plan_err!("LogicalPlan is not a CopyTo");
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn copy_to_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("COPY (values (1,2)) TO '{location}' STORED AS PARQUET
            OPTIONS ('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}')");
        copy_to_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region, use object_store defaults
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.region' '{region}', 'aws.session_token' '{session_token}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_oss() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.oss.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_cos() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "cos://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('aws.access_key_id' '{access_key_id}', 'aws.secret_access_key' '{secret_access_key}', 'aws.cos.endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\", \"private_key_id\":\"id\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.service_account_path' '{service_account_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('gcp.service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("No RSA key found in pem file"), "{err}");

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('gcp.application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_local_file() -> Result<()> {
        let location = "path/to/file.parquet";

        // Ensure that local files are also registered
        let sql =
            format!("CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'");
        create_external_table_test(location, &sql).await.unwrap();

        Ok(())
    }
}
