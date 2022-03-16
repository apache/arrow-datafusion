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

//! Execution plan for reading CSV files

use crate::error::{DataFusionError, Result};
use crate::execution::context::ExecutionContext;
use crate::physical_plan::ExecutionPlan;

use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use futures::{StreamExt, TryStreamExt};
use std::fs;
use std::path::Path;
use std::sync::Arc;
use tokio::task::{self, JoinHandle};

pub async fn plan_to_ipc(
    context: &ExecutionContext,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    writer_options: IpcWriteOptions,
) -> Result<()> {
    let path = path.as_ref();
    // create directory to contain the CSV files (one per partition)
    let fs_path = Path::new(path);
    let runtime = context.runtime_env();
    match fs::create_dir(fs_path) {
        Ok(()) => {
            let mut tasks = vec![];
            for i in 0..plan.output_partitioning().partition_count() {
                let plan = plan.clone();
                let filename = format!("part-{}.arrow", i);
                let path = fs_path.join(&filename);
                let file = fs::File::create(path)?;
                let stream = plan.execute(i, runtime.clone()).await?;
                let schema = stream.schema();
                let mut writer = FileWriter::try_new_with_options(
                    file,
                    schema.as_ref(),
                    writer_options.clone(),
                )?;
                let handle: JoinHandle<Result<()>> = task::spawn(async move {
                    stream
                        .map(|batch| writer.write(&batch?))
                        .try_collect()
                        .await
                        .map_err(DataFusionError::from)
                });
                tasks.push(handle);
            }
            futures::future::join_all(tasks).await;
            Ok(())
        }
        Err(e) => Err(DataFusionError::Execution(format!(
            "Could not create directory {}: {:?}",
            path, e
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    use arrow::datatypes::*;
    use arrow::ipc::writer::IpcWriteOptions;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    /// Generate CSV partitions within the supplied directory
    fn populate_csv_partitions(
        tmp_dir: &TempDir,
        partition_count: usize,
        file_extension: &str,
    ) -> Result<SchemaRef> {
        // define schema for data source (csv file)
        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
            Field::new("c3", DataType::Boolean, false),
        ]));

        // generate a partitioned file
        for partition in 0..partition_count {
            let filename = format!("partition-{}.{}", partition, file_extension);
            let file_path = tmp_dir.path().join(&filename);
            let mut file = File::create(file_path)?;

            // generate some data
            for i in 0..=10 {
                let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
                file.write_all(data.as_bytes())?;
            }
        }

        Ok(schema)
    }

    #[tokio::test]
    async fn write_ipc_results() -> Result<()> {
        // create partitioned input file and context
        let tmp_dir = TempDir::new()?;
        let mut ctx = ExecutionContext::with_config(
            ExecutionConfig::new().with_target_partitions(8),
        );

        let schema = populate_csv_partitions(&tmp_dir, 8, ".csv")?;

        // register csv file with the execution context
        ctx.register_csv(
            "test",
            tmp_dir.path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;

        // execute a simple query and write the results to CSV
        let out_dir = tmp_dir.as_ref().to_str().unwrap().to_string() + "/out";
        let df = ctx.sql("SELECT c1, c2 FROM test").await?;
        let opts = IpcWriteOptions::default();

        df.write_ipc(&out_dir, opts).await?;

        // create a new context and verify that the results were saved to a partitioned csv file
        let mut ctx = ExecutionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::UInt32, false),
            Field::new("c2", DataType::UInt64, false),
        ]));

        // register each partition as well as the top level dir
        let csv_read_option = CsvReadOptions::new().schema(&schema);
        ctx.register_csv(
            "part0",
            &format!("{}/part-0.arrow", out_dir),
            csv_read_option,
        )
        .await?;
        ctx.register_csv("allparts", &out_dir, csv_read_option)
            .await?;

        let part0 = ctx.sql("SELECT c1, c2 FROM part0").await?.collect().await?;
        let allparts = ctx
            .sql("SELECT c1, c2 FROM allparts")
            .await?
            .collect()
            .await?;

        let allparts_count: usize = allparts.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(part0[0].schema(), allparts[0].schema());

        assert_eq!(allparts_count, 80);

        Ok(())
    }
}
