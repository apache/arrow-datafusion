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

use crate::datasource::listing::ListingTableUrl;
use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures::StreamExt;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Executes a query and writes the results to a partitioned Parquet file.
pub async fn plan_to_parquet(
    task_ctx: Arc<TaskContext>,
    plan: Arc<dyn ExecutionPlan>,
    path: impl AsRef<str>,
    writer_properties: Option<WriterProperties>,
) -> datafusion_common::Result<()> {
    let path = path.as_ref();
    let parsed = ListingTableUrl::parse(path)?;
    let object_store_url = parsed.object_store();
    let store = task_ctx.runtime_env().object_store(&object_store_url)?;
    let mut join_set = JoinSet::new();
    for i in 0..plan.output_partitioning().partition_count() {
        let plan: Arc<dyn ExecutionPlan> = Arc::clone(&plan);
        let filename = format!("{}/part-{i}.parquet", parsed.prefix());
        let file = Path::parse(filename)?;
        let propclone = writer_properties.clone();

        let storeref = Arc::clone(&store);
        let buf_writer = BufWriter::new(storeref, file.clone());
        let mut stream = plan.execute(i, Arc::clone(&task_ctx))?;
        join_set.spawn(async move {
            let mut writer =
                AsyncArrowWriter::try_new(buf_writer, plan.schema(), propclone)?;
            while let Some(next_batch) = stream.next().await {
                let batch = next_batch?;
                writer.write(&batch).await?;
            }
            writer
                .close()
                .await
                .map_err(DataFusionError::from)
                .map(|_| ())
        });
    }

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(res) => res?,
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }
    }

    Ok(())
}
