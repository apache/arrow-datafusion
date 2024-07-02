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

//! Execution plan for reading Arrow files

use std::any::Any;
use std::sync::Arc;

use super::FileGroupPartitioner;
use crate::datasource::listing::PartitionedFile;
use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig,
};
use crate::error::Result;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};

use arrow_ipc::reader::FileDecoder;
use arrow_schema::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Statistics;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion_physical_plan::{ExecutionMode, PlanProperties};

use futures::StreamExt;
use itertools::Itertools;
use object_store::{GetOptions, GetRange, GetResultPayload, ObjectStore};

/// Execution plan for scanning Arrow data source
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArrowExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
}

impl ArrowExec {
    /// Create a new Arrow reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();
        let cache = Self::compute_properties(
            Arc::clone(&projected_schema),
            &projected_output_ordering,
            &base_config,
        );
        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }

    fn output_partitioning_helper(file_scan_config: &FileScanConfig) -> Partitioning {
        Partitioning::UnknownPartitioning(file_scan_config.file_groups.len())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        projected_output_ordering: &[LexOrdering],
        file_scan_config: &FileScanConfig,
    ) -> PlanProperties {
        // Equivalence Properties
        let eq_properties =
            EquivalenceProperties::new_with_orderings(schema, projected_output_ordering);

        PlanProperties::new(
            eq_properties,
            Self::output_partitioning_helper(file_scan_config), // Output Partitioning
            ExecutionMode::Bounded,                             // Execution Mode
        )
    }

    fn with_file_groups(mut self, file_groups: Vec<Vec<PartitionedFile>>) -> Self {
        self.base_config.file_groups = file_groups;
        // Changing file groups may invalidate output partitioning. Update it also
        let output_partitioning = Self::output_partitioning_helper(&self.base_config);
        self.cache = self.cache.with_partitioning(output_partitioning);
        self
    }
}

impl DisplayAs for ArrowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ArrowExec: ")?;
        self.base_config.fmt_as(t, f)
    }
}

impl ExecutionPlan for ArrowExec {
    fn name(&self) -> &'static str {
        "ArrowExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// Redistribute files across partitions according to their size
    /// See comments on [`FileGroupPartitioner`] for more detail.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let repartition_file_min_size = config.optimizer.repartition_file_min_size;
        let repartitioned_file_groups_option = FileGroupPartitioner::new()
            .with_target_partitions(target_partitions)
            .with_repartition_file_min_size(repartition_file_min_size)
            .with_preserve_order_within_groups(
                self.properties().output_ordering().is_some(),
            )
            .repartition_file_groups(&self.base_config.file_groups);

        if let Some(repartitioned_file_groups) = repartitioned_file_groups_option {
            let mut new_plan = self.clone();
            new_plan = new_plan.with_file_groups(repartitioned_file_groups);
            return Ok(Some(Arc::new(new_plan)));
        }
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use super::file_stream::FileStream;
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let opener = ArrowOpener {
            object_store,
            projection: self.base_config.file_column_projection_indices(),
        };
        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

pub struct ArrowOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub projection: Option<Vec<usize>>,
}

impl FileOpener for ArrowOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let range = file_meta.range.clone();
            match range {
                None => {
                    let r = object_store.get(file_meta.location()).await?;
                    match r.payload {
                        GetResultPayload::File(file, _) => {
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                file, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader).boxed())
                        }
                        GetResultPayload::Stream(_) => {
                            let bytes = r.bytes().await?;
                            let cursor = std::io::Cursor::new(bytes);
                            let arrow_reader = arrow::ipc::reader::FileReader::try_new(
                                cursor, projection,
                            )?;
                            Ok(futures::stream::iter(arrow_reader).boxed())
                        }
                    }
                }
                Some(range) => {
                    // range is not none, the file maybe split into multiple parts to scan in parallel
                    // get footer_len firstly
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10)),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(file_meta.location(), get_option)
                        .await?;
                    let footer_len_buf = get_result.bytes().await?;
                    let footer_len = arrow_ipc::reader::read_footer_length(
                        footer_len_buf[..].try_into().unwrap(),
                    )?;
                    // read footer according to footer_len
                    let get_option = GetOptions {
                        range: Some(GetRange::Suffix(10 + footer_len)),
                        ..Default::default()
                    };
                    let get_result = object_store
                        .get_opts(file_meta.location(), get_option)
                        .await?;
                    let footer_buf = get_result.bytes().await?;
                    let footer = arrow_ipc::root_as_footer(
                        footer_buf[..footer_len].try_into().unwrap(),
                    )
                    .map_err(|err| {
                        arrow_schema::ArrowError::ParseError(format!(
                            "Unable to get root as footer: {err:?}"
                        ))
                    })?;
                    // build decoder according to footer & projection
                    let schema =
                        arrow_ipc::convert::fb_to_schema(footer.schema().unwrap());
                    let mut decoder = FileDecoder::new(schema.into(), footer.version());
                    if let Some(projection) = projection {
                        decoder = decoder.with_projection(projection);
                    }
                    let dict_ranges = footer
                        .dictionaries()
                        .iter()
                        .flatten()
                        .map(|block| {
                            let block_len = block.bodyLength() as usize
                                + block.metaDataLength() as usize;
                            let block_offset = block.offset() as usize;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();
                    let dict_results = object_store
                        .get_ranges(file_meta.location(), &dict_ranges)
                        .await?;
                    for (dict_block, dict_result) in
                        footer.dictionaries().iter().flatten().zip(dict_results)
                    {
                        decoder.read_dictionary(dict_block, &dict_result.into())?;
                    }

                    // filter recordbatches according to range
                    let recordbatches = footer
                        .recordBatches()
                        .iter()
                        .flatten()
                        .filter(|block| {
                            let block_offset = block.offset() as usize;
                            block_offset >= range.start as usize
                                && block_offset < range.end as usize
                        })
                        .copied()
                        .collect_vec();

                    let recordbatch_ranges = recordbatches
                        .iter()
                        .map(|block| {
                            let block_len = block.bodyLength() as usize
                                + block.metaDataLength() as usize;
                            let block_offset = block.offset() as usize;
                            block_offset..block_offset + block_len
                        })
                        .collect_vec();

                    let recordbatch_results = object_store
                        .get_ranges(file_meta.location(), &recordbatch_ranges)
                        .await?;

                    Ok(futures::stream::iter(
                        recordbatches
                            .into_iter()
                            .zip(recordbatch_results)
                            .filter_map(move |(block, data)| {
                                match decoder.read_record_batch(&block, &data.into()) {
                                    Ok(Some(record_batch)) => Some(Ok(record_batch)),
                                    Ok(None) => None,
                                    Err(err) => Some(Err(err)),
                                }
                            }),
                    )
                    .boxed())
                }
            }
        }))
    }
}
