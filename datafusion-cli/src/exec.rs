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

use crate::{
    command::{Command, OutputFormat},
    helper::{unescape_input, CliHelper},
    object_storage::{
        get_gcs_object_store_builder, get_oss_object_store_builder,
        get_s3_object_store_builder,
    },
    print_options::{MaxRows, PrintOptions},
};
use datafusion::common::plan_datafusion_err;
use datafusion::sql::{parser::DFParser, sqlparser::dialect::dialect_from_str};
use datafusion::{
    datasource::listing::ListingTableUrl,
    error::{DataFusionError, Result},
    logical_expr::{CreateExternalTable, DdlStatement},
};
use datafusion::{logical_expr::LogicalPlan, prelude::SessionContext};
use object_store::ObjectStore;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::io::prelude::*;
use std::io::BufReader;
use std::time::Instant;
use std::{fs::File, sync::Arc};
use url::Url;

/// run and execute SQL statements and commands, against a context with the given print options
pub async fn exec_from_commands(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    commands: Vec<String>,
) {
    for sql in commands {
        match exec_and_print(ctx, print_options, sql).await {
            Ok(_) => {}
            Err(err) => println!("{err}"),
        }
    }
}

/// run and execute SQL statements and commands from a file, against a context with the given print options
pub async fn exec_from_lines(
    ctx: &mut SessionContext,
    reader: &mut BufReader<File>,
    print_options: &PrintOptions,
) {
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
        match exec_and_print(ctx, print_options, query).await {
            Ok(_) => {}
            Err(err) => println!("{err}"),
        }
    }
}

pub async fn exec_from_files(
    files: Vec<String>,
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
) {
    let files = files
        .into_iter()
        .map(|file_path| File::open(file_path).unwrap())
        .collect::<Vec<_>>();
    for file in files {
        let mut reader = BufReader::new(file);
        exec_from_lines(ctx, &mut reader, print_options).await;
    }
}

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(
    ctx: &mut SessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(CliHelper::new(
        &ctx.task_ctx().session_config().options().sql_parser.dialect,
    )));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

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
                                    if let Err(e) =
                                        command.execute(&mut print_options).await
                                    {
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
                            if let Err(e) = cmd.execute(ctx, &mut print_options).await {
                                eprintln!("{e}")
                            }
                        }
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            }
            Ok(line) => {
                rl.add_history_entry(line.trim_end())?;
                match exec_and_print(ctx, &print_options, line).await {
                    Ok(_) => {}
                    Err(err) => eprintln!("{err}"),
                }
                // dialect might have changed
                rl.helper_mut().unwrap().set_dialect(
                    &ctx.task_ctx().session_config().options().sql_parser.dialect,
                );
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
        let plan = ctx.state().statement_to_plan(statement).await?;

        // For plans like `Explain` ignore `MaxRows` option and always display all rows
        let should_ignore_maxrows = matches!(
            plan,
            LogicalPlan::Explain(_)
                | LogicalPlan::DescribeTable(_)
                | LogicalPlan::Analyze(_)
        );

        let df = match &plan {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
                create_external_table(ctx, cmd).await?;
                ctx.execute_logical_plan(plan).await?
            }
            _ => ctx.execute_logical_plan(plan).await?,
        };

        let results = df.collect().await?;

        let print_options = if should_ignore_maxrows {
            PrintOptions {
                maxrows: MaxRows::Unlimited,
                ..print_options.clone()
            }
        } else {
            print_options.clone()
        };
        print_options.print_batches(&results, now)?;
    }

    Ok(())
}

async fn create_external_table(
    ctx: &SessionContext,
    cmd: &CreateExternalTable,
) -> Result<()> {
    let table_path = ListingTableUrl::parse(&cmd.location)?;
    let scheme = table_path.scheme();
    let url: &Url = table_path.as_ref();

    // registering the cloud object store dynamically using cmd.options
    let store = match scheme {
        "s3" => {
            let builder = get_s3_object_store_builder(url, cmd).await?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "oss" => {
            let builder = get_oss_object_store_builder(url, cmd)?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        "gs" | "gcs" => {
            let builder = get_gcs_object_store_builder(url, cmd)?;
            Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        }
        _ => {
            // for other types, try to get from the object_store_registry
            ctx.runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Unsupported object store scheme: {}",
                        scheme
                    ))
                })?
        }
    };

    ctx.runtime_env().register_object_store(url, store);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::plan_err;

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(sql).await?;

        if let LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) = &plan {
            create_external_table(&ctx, cmd).await?;
        } else {
            return plan_err!("LogicalPlan is not a CreateExternalTable");
        }

        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Missing region"));

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'region' '{region}', 'session_token' '{session_token}') LOCATION '{location}'");
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
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('service_account_path' '{service_account_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("os error 2"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No RSA key found in pem file"));

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
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
