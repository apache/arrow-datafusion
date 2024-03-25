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

//! Command within CLI

use crate::exec::exec_from_lines;
use crate::functions::{display_all_functions, Function};
use crate::print_format::PrintFormat;
use crate::print_options::PrintOptions;
use clap::ValueEnum;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::exec_err;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion_common::instant::Instant;
use std::fs::File;
use std::io::BufReader;
use std::str::FromStr;
use std::sync::Arc;

/// Command
#[derive(Debug)]
pub enum Command {
    Quit,
    Help,
    ListTables,
    DescribeTableStmt(String),
    ListFunctions,
    Include(Option<String>),
    SearchFunctions(String),
    QuietMode(Option<bool>),
    OutputFormat(Option<String>),
}

pub enum OutputFormat {
    ChangeFormat(String),
}

impl Command {
    pub async fn execute(
        &self,
        ctx: &mut SessionContext,
        print_options: &mut PrintOptions,
    ) -> Result<()> {
        let now = Instant::now();
        match self {
            Self::Help => print_options.print_batches(&[all_commands_info()], now),
            Self::ListTables => {
                let df = ctx.sql("SHOW TABLES").await?;
                let batches = df.collect().await?;
                print_options.print_batches(&batches, now)
            }
            Self::DescribeTableStmt(name) => {
                let df = ctx.sql(&format!("SHOW COLUMNS FROM {}", name)).await?;
                let batches = df.collect().await?;
                print_options.print_batches(&batches, now)
            }
            Self::Include(filename) => {
                if let Some(filename) = filename {
                    let file = File::open(filename).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Error opening {:?} {}",
                            filename, e
                        ))
                    })?;
                    exec_from_lines(ctx, &mut BufReader::new(file), print_options)
                        .await?;
                    Ok(())
                } else {
                    exec_err!("Required filename argument is missing")
                }
            }
            Self::QuietMode(quiet) => {
                if let Some(quiet) = quiet {
                    print_options.quiet = *quiet;
                    println!(
                        "Quiet mode set to {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                } else {
                    println!(
                        "Quiet mode is {}",
                        if print_options.quiet { "true" } else { "false" }
                    );
                }
                Ok(())
            }
            Self::Quit => exec_err!("Unexpected quit, this should be handled outside"),
            Self::ListFunctions => display_all_functions(),
            Self::SearchFunctions(function) => {
                if let Ok(func) = function.parse::<Function>() {
                    let details = func.function_details()?;
                    println!("{}", details);
                    Ok(())
                } else {
                    exec_err!("{function} is not a supported function")
                }
            }
            Self::OutputFormat(_) => exec_err!(
                "Unexpected change output format, this should be handled outside"
            ),
        }
    }

    fn get_name_and_description(&self) -> (&'static str, &'static str) {
        match self {
            Self::Quit => ("\\q", "quit datafusion-cli"),
            Self::ListTables => ("\\d", "list tables"),
            Self::DescribeTableStmt(_) => ("\\d name", "describe table"),
            Self::Help => ("\\?", "help"),
            Self::Include(_) => {
                ("\\i filename", "reads input from the specified filename")
            }
            Self::ListFunctions => ("\\h", "function list"),
            Self::SearchFunctions(_) => ("\\h function", "search function"),
            Self::QuietMode(_) => ("\\quiet (true|false)?", "print or set quiet mode"),
            Self::OutputFormat(_) => {
                ("\\pset [NAME [VALUE]]", "set table output option\n(format)")
            }
        }
    }
}

const ALL_COMMANDS: [Command; 9] = [
    Command::ListTables,
    Command::DescribeTableStmt(String::new()),
    Command::Quit,
    Command::Help,
    Command::Include(Some(String::new())),
    Command::ListFunctions,
    Command::SearchFunctions(String::new()),
    Command::QuietMode(None),
    Command::OutputFormat(None),
];

fn all_commands_info() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("Command", DataType::Utf8, false),
        Field::new("Description", DataType::Utf8, false),
    ]));
    let (names, description): (Vec<&str>, Vec<&str>) = ALL_COMMANDS
        .into_iter()
        .map(|c| c.get_name_and_description())
        .unzip();
    RecordBatch::try_new(
        schema,
        [names, description]
            .into_iter()
            .map(|i| Arc::new(StringArray::from(i)) as ArrayRef)
            .collect::<Vec<_>>(),
    )
    .expect("This should not fail")
}

impl FromStr for Command {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (c, arg) = if let Some((a, b)) = s.split_once(' ') {
            (a, Some(b))
        } else {
            (s, None)
        };
        Ok(match (c, arg) {
            ("q", None) => Self::Quit,
            ("d", None) => Self::ListTables,
            ("d", Some(name)) => Self::DescribeTableStmt(name.into()),
            ("?", None) => Self::Help,
            ("h", None) => Self::ListFunctions,
            ("h", Some(function)) => Self::SearchFunctions(function.into()),
            ("i", None) => Self::Include(None),
            ("i", Some(filename)) => Self::Include(Some(filename.to_owned())),
            ("quiet", Some("true" | "t" | "yes" | "y" | "on")) => {
                Self::QuietMode(Some(true))
            }
            ("quiet", Some("false" | "f" | "no" | "n" | "off")) => {
                Self::QuietMode(Some(false))
            }
            ("quiet", None) => Self::QuietMode(None),
            ("pset", Some(subcommand)) => {
                Self::OutputFormat(Some(subcommand.to_string()))
            }
            ("pset", None) => Self::OutputFormat(None),
            _ => return Err(()),
        })
    }
}

impl FromStr for OutputFormat {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (c, arg) = if let Some((a, b)) = s.split_once(' ') {
            (a, Some(b))
        } else {
            (s, None)
        };
        Ok(match (c, arg) {
            ("format", Some(format)) => Self::ChangeFormat(format.to_string()),
            _ => return Err(()),
        })
    }
}

impl OutputFormat {
    pub async fn execute(&self, print_options: &mut PrintOptions) -> Result<()> {
        match self {
            Self::ChangeFormat(format) => {
                if let Ok(format) = format.parse::<PrintFormat>() {
                    print_options.format = format;
                    println!("Output format is {:?}.", print_options.format);
                    Ok(())
                } else {
                    exec_err!(
                        "{:?} is not a valid format type [possible values: {:?}]",
                        format,
                        PrintFormat::value_variants()
                    )
                }
            }
        }
    }
}
