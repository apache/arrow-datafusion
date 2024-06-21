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

#[cfg(not(target_os = "windows"))]
mod non_windows {
    use datafusion::assert_batches_eq;
    use datafusion_common::instant::Instant;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SchemaRef;
    use futures::StreamExt;
    use nix::sys::stat;
    use nix::unistd;
    use tempfile::TempDir;
    use tokio::task::JoinSet;

    use datafusion::datasource::stream::{FileStreamProvider, StreamConfig, StreamTable};
    use datafusion::datasource::TableProvider;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_common::{exec_err, Result};
    use datafusion_expr::Expr;

    // Number of lines written to FIFO
    const TEST_BATCH_SIZE: usize = 5;
    const TEST_DATA_SIZE: usize = 5;

    /// Makes a TableProvider for a fifo file using `StreamTable` with the `StreamProvider` trait
    fn fifo_table(
        schema: SchemaRef,
        path: impl Into<PathBuf>,
        sort: Vec<Vec<Expr>>,
    ) -> Arc<dyn TableProvider> {
        let source = FileStreamProvider::new_file(schema, path.into())
            .with_batch_size(TEST_BATCH_SIZE)
            .with_header(true);
        let config = StreamConfig::new(Arc::new(source)).with_order(sort);
        Arc::new(StreamTable::new(Arc::new(config)))
    }

    fn create_fifo_file(tmp_dir: &TempDir, file_name: &str) -> Result<PathBuf> {
        let file_path = tmp_dir.path().join(file_name);
        // Simulate an infinite environment via a FIFO file
        if let Err(e) = unistd::mkfifo(&file_path, stat::Mode::S_IRWXU) {
            exec_err!("{}", e)
        } else {
            Ok(file_path)
        }
    }

    fn write_to_fifo(
        mut file: &File,
        line: &str,
        ref_time: Instant,
        broken_pipe_timeout: Duration,
    ) -> Result<()> {
        // We need to handle broken pipe error until the reader is ready. This
        // is why we use a timeout to limit the wait duration for the reader.
        // If the error is different than broken pipe, we fail immediately.
        while let Err(e) = file.write_all(line.as_bytes()) {
            if e.raw_os_error().unwrap() == 32 {
                let interval = Instant::now().duration_since(ref_time);
                if interval < broken_pipe_timeout {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }
            return exec_err!("{}", e);
        }
        Ok(())
    }

    fn create_writing_thread(
        file_path: PathBuf,
        maybe_header: Option<String>,
        lines: Vec<String>,
        waiting_lock: Arc<AtomicBool>,
        wait_until: usize,
        tasks: &mut JoinSet<()>,
    ) {
        // Timeout for a long period of BrokenPipe error
        let broken_pipe_timeout = Duration::from_secs(10);
        let sa = file_path.clone();
        // Spawn a new thread to write to the FIFO file
        #[allow(clippy::disallowed_methods)] // spawn allowed only in tests
        tasks.spawn_blocking(move || {
            let file = OpenOptions::new().write(true).open(sa).unwrap();
            // Reference time to use when deciding to fail the test
            let execution_start = Instant::now();
            if let Some(header) = maybe_header {
                write_to_fifo(&file, &header, execution_start, broken_pipe_timeout)
                    .unwrap();
            }
            for (cnt, line) in lines.iter().enumerate() {
                while waiting_lock.load(Ordering::SeqCst) && cnt > wait_until {
                    thread::sleep(Duration::from_millis(50));
                }
                write_to_fifo(&file, line, execution_start, broken_pipe_timeout).unwrap();
            }
            drop(file);
        });
    }

    /// This example demonstrates a scanning against an Arrow data source (JSON) and
    /// fetching results
    pub async fn main() -> Result<()> {
        // Create session context
        let config = SessionConfig::new()
            .with_batch_size(TEST_BATCH_SIZE)
            .with_collect_statistics(false)
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        let tmp_dir = TempDir::new()?;
        let fifo_path = create_fifo_file(&tmp_dir, "fifo_unbounded.csv")?;

        let mut tasks: JoinSet<()> = JoinSet::new();
        let waiting = Arc::new(AtomicBool::new(true));

        let data_iter = 0..TEST_DATA_SIZE;
        let lines = data_iter
            .map(|i| format!("{},{}\n", i, i + 1))
            .collect::<Vec<_>>();

        create_writing_thread(
            fifo_path.clone(),
            Some("a1,a2\n".to_owned()),
            lines.clone(),
            waiting.clone(),
            TEST_DATA_SIZE,
            &mut tasks,
        );

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a1", DataType::UInt32, false),
            Field::new("a2", DataType::UInt32, false),
        ]));

        // Specify the ordering:
        let order = vec![vec![datafusion_expr::col("a1").sort(true, false)]];

        let provider = fifo_table(schema.clone(), fifo_path, order.clone());
        ctx.register_table("fifo", provider)?;

        let df = ctx.sql("SELECT * FROM fifo").await.unwrap();
        let mut stream = df.execute_stream().await.unwrap();

        let mut batches = Vec::new();
        if let Some(Ok(batch)) = stream.next().await {
            batches.push(batch)
        }

        let expected = vec![
            "+----+----+",
            "| a1 | a2 |",
            "+----+----+",
            "| 0  | 1  |",
            "| 1  | 2  |",
            "| 2  | 3  |",
            "| 3  | 4  |",
            "| 4  | 5  |",
            "+----+----+",
        ];

        assert_batches_eq!(&expected, &batches);

        Ok(())
    }
}

#[tokio::main]
async fn main() -> datafusion_common::Result<()> {
    #[cfg(target_os = "windows")]
    {
        println!("file_stream_provider example does not work on windows");
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    {
        non_windows::main().await
    }
}
