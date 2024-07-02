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

//! This module contains tests for limiting memory at runtime in DataFusion

use arrow::datatypes::{Int32Type, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, DictionaryArray};
use arrow_schema::SortOptions;
use async_trait::async_trait;
use datafusion::assert_batches_eq;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use futures::StreamExt;
use std::any::Any;
use std::sync::{Arc, OnceLock};

use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::context::SessionState;
use datafusion::execution::disk_manager::DiskManagerConfig;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion_common::{assert_contains, Result};

use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::logical_type::schema::LogicalSchemaRef;
use datafusion_execution::TaskContext;
use test_utils::AccessLogGenerator;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}

#[tokio::test]
async fn oom_sort() {
    TestCase::new()
        .with_query("select * from t order by host DESC")
        .with_expected_errors(vec![
            "Resources exhausted: Memory Exhausted while Sorting (DiskManager is disabled)",
        ])
        .with_memory_limit(200_000)
        .run()
        .await
}

#[tokio::test]
async fn group_by_none() {
    TestCase::new()
        .with_query("select median(request_bytes) from t")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "AggregateStream",
        ])
        .with_memory_limit(2_000)
        .run()
        .await
}

#[tokio::test]
async fn group_by_row_hash() {
    TestCase::new()
        .with_query("select count(*) from t GROUP BY response_bytes")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ])
        .with_memory_limit(2_000)
        .run()
        .await
}

#[tokio::test]
async fn group_by_hash() {
    TestCase::new()
        // group by dict column
        .with_query("select count(*) from t GROUP BY service, host, pod, container")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "GroupedHashAggregateStream",
        ])
        .with_memory_limit(1_000)
        .run()
        .await
}

#[tokio::test]
async fn join_by_key_multiple_partitions() {
    let config = SessionConfig::new().with_target_partitions(2);
    TestCase::new()
        .with_query("select t1.* from t t1 JOIN t t2 ON t1.service = t2.service")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput[0]",
        ])
        .with_memory_limit(1_000)
        .with_config(config)
        .run()
        .await
}

#[tokio::test]
async fn join_by_key_single_partition() {
    let config = SessionConfig::new().with_target_partitions(1);
    TestCase::new()
        .with_query("select t1.* from t t1 JOIN t t2 ON t1.service = t2.service")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "HashJoinInput",
        ])
        .with_memory_limit(1_000)
        .with_config(config)
        .run()
        .await
}

#[tokio::test]
async fn join_by_expression() {
    TestCase::new()
        .with_query("select t1.* from t t1 JOIN t t2 ON t1.service != t2.service")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "NestedLoopJoinLoad[0]",
        ])
        .with_memory_limit(1_000)
        .run()
        .await
}

#[tokio::test]
async fn cross_join() {
    TestCase::new()
        .with_query("select t1.* from t t1 CROSS JOIN t t2")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "CrossJoinExec",
        ])
        .with_memory_limit(1_000)
        .run()
        .await
}

#[tokio::test]
async fn merge_join() {
    // Planner chooses MergeJoin only if number of partitions > 1
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.prefer_hash_join", false);

    TestCase::new()
        .with_query(
            "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        )
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "SMJStream",
        ])
        .with_memory_limit(1_000)
        .with_config(config)
        .run()
        .await
}

#[tokio::test]
async fn symmetric_hash_join() {
    TestCase::new()
        .with_query(
            "select t1.* from t t1 JOIN t t2 ON t1.pod = t2.pod AND t1.time = t2.time",
        )
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "SymmetricHashJoinStream",
        ])
        .with_memory_limit(1_000)
        .with_scenario(Scenario::AccessLogStreaming)
        .run()
        .await
}

#[tokio::test]
async fn sort_preserving_merge() {
    let scenario = Scenario::new_dictionary_strings(2);
    let partition_size = scenario.partition_size();

    TestCase::new()
    // This query uses the exact same ordering as the input table
    // so only a merge is needed
        .with_query("select * from t ORDER BY a ASC NULLS LAST, b ASC NULLS LAST LIMIT 10")
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "SortPreservingMergeExec",
        ])
        // provide insufficient memory to merge
        .with_memory_limit(partition_size / 2)
        // two partitions of data, so a merge is required
        .with_scenario(scenario)
        .with_expected_plan(
            // It is important that this plan only has
            // SortPreservingMergeExec (not a Sort which would compete
            // with the SortPreservingMergeExec for memory)
            &[
                "+---------------+-------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                        |",
                "+---------------+-------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Limit: skip=0, fetch=10                                                                                     |",
                "|               |   Sort: t.a ASC NULLS LAST, t.b ASC NULLS LAST, fetch=10                                                    |",
                "|               |     TableScan: t projection=[a, b]                                                                          |",
                "| physical_plan | GlobalLimitExec: skip=0, fetch=10                                                                           |",
                "|               |   SortPreservingMergeExec: [a@0 ASC NULLS LAST,b@1 ASC NULLS LAST], fetch=10                                |",
                "|               |     MemoryExec: partitions=2, partition_sizes=[5, 5], output_ordering=a@0 ASC NULLS LAST,b@1 ASC NULLS LAST |",
                "|               |                                                                                                             |",
                "+---------------+-------------------------------------------------------------------------------------------------------------+",
            ]
        )
        .run()
        .await
}

#[tokio::test]
async fn sort_spill_reservation() {
    let scenario = Scenario::new_dictionary_strings(1);
    let partition_size = scenario.partition_size();

    let base_config = SessionConfig::new()
        // do not allow the sort to use the 'concat in place' path
        .with_sort_in_place_threshold_bytes(10);

    // This test case shows how sort_spill_reservation works by
    // purposely sorting data that requires non trivial memory to
    // sort/merge.
    let test = TestCase::new()
    // This query uses a different order than the input table to
    // force a sort. It also needs to have multiple columns to
    // force RowFormat / interner that makes merge require
    // substantial memory
        .with_query("select * from t ORDER BY a , b DESC")
    // enough memory to sort if we don't try to merge it all at once
        .with_memory_limit(partition_size)
    // use a single partiton so only a sort is needed
        .with_scenario(scenario)
        .with_disk_manager_config(DiskManagerConfig::NewOs)
        .with_expected_plan(
            // It is important that this plan only has a SortExec, not
            // also merge, so we can ensure the sort could finish
            // given enough merging memory
            &[
                "+---------------+--------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                   |",
                "+---------------+--------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Sort: t.a ASC NULLS LAST, t.b DESC NULLS FIRST                                                         |",
                "|               |   TableScan: t projection=[a, b]                                                                       |",
                "| physical_plan | SortExec: expr=[a@0 ASC NULLS LAST,b@1 DESC], preserve_partitioning=[false]                            |",
                "|               |   MemoryExec: partitions=1, partition_sizes=[5], output_ordering=a@0 ASC NULLS LAST,b@1 ASC NULLS LAST |",
                "|               |                                                                                                        |",
                "+---------------+--------------------------------------------------------------------------------------------------------+",
            ]
        );

    let config = base_config
        .clone()
        // provide insufficient reserved space for merging,
        // the sort will fail while trying to merge
        .with_sort_spill_reservation_bytes(1024);

    test.clone()
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "ExternalSorterMerge", // merging in sort fails
        ])
        .with_config(config)
        .run()
        .await;

    let config = base_config
        // reserve sufficient space up front for merge and this time,
        // which will force the spills to happen with less buffered
        // input and thus with enough to merge.
        .with_sort_spill_reservation_bytes(partition_size / 2);

    test.with_config(config).with_expected_success().run().await;
}

#[tokio::test]
async fn oom_recursive_cte() {
    TestCase::new()
        .with_query(
            "WITH RECURSIVE nodes AS (
            SELECT 1 as id
            UNION ALL
            SELECT UNNEST(RANGE(id+1, id+1000)) as id
            FROM nodes
            WHERE id < 10
        )
        SELECT * FROM nodes;",
        )
        .with_expected_errors(vec![
            "Resources exhausted: Failed to allocate additional",
            "RecursiveQuery",
        ])
        .with_memory_limit(2_000)
        .run()
        .await
}

/// Run the query with the specified memory limit,
/// and verifies the expected errors are returned
#[derive(Clone, Debug)]
struct TestCase {
    query: Option<String>,
    expected_errors: Vec<String>,
    memory_limit: usize,
    config: SessionConfig,
    scenario: Scenario,
    /// How should the disk manager (that allows spilling) be
    /// configured? Defaults to `Disabled`
    disk_manager_config: DiskManagerConfig,
    /// Expected explain plan, if non emptry
    expected_plan: Vec<String>,
    /// Is the plan expected to pass? Defaults to false
    expected_success: bool,
}

impl TestCase {
    fn new() -> Self {
        Self {
            query: None,
            expected_errors: vec![],
            memory_limit: 0,
            config: SessionConfig::new(),
            scenario: Scenario::AccessLog,
            disk_manager_config: DiskManagerConfig::Disabled,
            expected_plan: vec![],
            expected_success: false,
        }
    }

    /// Set the query to run
    fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Set a list of expected strings that must appear in any errors
    fn with_expected_errors<'a>(
        mut self,
        expected_errors: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        self.expected_errors =
            expected_errors.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set the amount of memory that can be used
    fn with_memory_limit(mut self, memory_limit: usize) -> Self {
        self.memory_limit = memory_limit;
        self
    }

    /// Specify the configuration to use
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = config;
        self
    }

    /// Mark that the test expects the query to run successfully
    pub fn with_expected_success(mut self) -> Self {
        self.expected_success = true;
        self
    }

    /// Specify the scenario to run
    pub fn with_scenario(mut self, scenario: Scenario) -> Self {
        self.scenario = scenario;
        self
    }

    /// Specify if the disk manager should be enabled. If true,
    /// operators that support it can spill
    pub fn with_disk_manager_config(
        mut self,
        disk_manager_config: DiskManagerConfig,
    ) -> Self {
        self.disk_manager_config = disk_manager_config;
        self
    }

    /// Specify an expected plan to review
    pub fn with_expected_plan(mut self, expected_plan: &[&str]) -> Self {
        self.expected_plan = expected_plan.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Run the test, panic'ing on error
    async fn run(self) {
        let Self {
            query,
            expected_errors,
            memory_limit,
            config,
            scenario,
            disk_manager_config,
            expected_plan,
            expected_success,
        } = self;

        let table = scenario.table();

        let rt_config = RuntimeConfig::new()
            // do not allow spilling
            .with_disk_manager(disk_manager_config)
            .with_memory_limit(memory_limit, MEMORY_FRACTION);

        let runtime = RuntimeEnv::new(rt_config).unwrap();

        // Configure execution
        let state = SessionState::new_with_config_rt(config, Arc::new(runtime));
        let state = match scenario.rules() {
            Some(rules) => state.with_physical_optimizer_rules(rules),
            None => state,
        };

        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("t", table).expect("registering table");

        let query = query.expect("Test error: query not specified");
        let df = ctx.sql(&query).await.expect("Planning query");

        if !expected_plan.is_empty() {
            let expected_plan: Vec<_> =
                expected_plan.iter().map(|s| s.as_str()).collect();
            let actual_plan = df
                .clone()
                .explain(false, false)
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_batches_eq!(expected_plan, &actual_plan);
        }

        match df.collect().await {
            Ok(_batches) => {
                if !expected_success {
                    panic!(
                        "Unexpected success when running, expected memory limit failure"
                    )
                }
            }
            Err(e) => {
                if expected_success {
                    panic!(
                        "Unexpected failure when running, expected success but got: {e}"
                    )
                } else {
                    for error_substring in expected_errors {
                        assert_contains!(e.to_string(), error_substring);
                    }
                }
            }
        }
    }
}

/// 50 byte memory limit
const MEMORY_FRACTION: f64 = 0.95;

/// Different data scenarios
#[derive(Clone, Debug)]
enum Scenario {
    /// 1000 rows of access log data with batches of 50 rows
    AccessLog,

    /// 1000 rows of access log data with batches of 50 rows in a
    /// [`StreamingTable`]
    AccessLogStreaming,

    /// N partitions of sorted, dictionary encoded strings.
    DictionaryStrings {
        partitions: usize,
        /// If true, splits all input batches into 1 row each
        single_row_batches: bool,
    },
}

impl Scenario {
    /// Create a new DictionaryStrings scenario with the number of partitions
    fn new_dictionary_strings(partitions: usize) -> Self {
        Self::DictionaryStrings {
            partitions,
            single_row_batches: false,
        }
    }

    /// return the size, in bytes, of each partition
    fn partition_size(&self) -> usize {
        if let Self::DictionaryStrings {
            single_row_batches, ..
        } = self
        {
            batches_byte_size(&maybe_split_batches(dict_batches(), *single_row_batches))
        } else {
            panic!("Scenario does not support partition size");
        }
    }

    /// return a TableProvider with data for the test
    fn table(&self) -> Arc<dyn TableProvider> {
        match self {
            Self::AccessLog => {
                let batches = access_log_batches();
                let table =
                    MemTable::try_new(batches[0].schema(), vec![batches]).unwrap();
                Arc::new(table)
            }
            Self::AccessLogStreaming => {
                let batches = access_log_batches();

                // Create a new streaming table with the generated schema and batches
                let table = StreamingTable::try_new(
                    batches[0].schema(),
                    vec![Arc::new(DummyStreamPartition {
                        schema: batches[0].schema(),
                        batches: batches.clone(),
                    })],
                )
                .unwrap()
                .with_infinite_table(true);
                Arc::new(table)
            }
            Self::DictionaryStrings {
                partitions,
                single_row_batches,
            } => {
                use datafusion::physical_expr::expressions::col;
                let batches: Vec<Vec<_>> = std::iter::repeat(maybe_split_batches(
                    dict_batches(),
                    *single_row_batches,
                ))
                .take(*partitions)
                .collect();

                let schema = batches[0][0].schema();
                let options = SortOptions {
                    descending: false,
                    nulls_first: false,
                };
                let sort_information = vec![vec![
                    PhysicalSortExpr {
                        expr: col("a", &schema).unwrap(),
                        options,
                    },
                    PhysicalSortExpr {
                        expr: col("b", &schema).unwrap(),
                        options,
                    },
                ]];

                let table = SortedTableProvider::new(batches, sort_information);
                Arc::new(table)
            }
        }
    }

    /// return specific physical optimizer rules to use
    fn rules(&self) -> Option<Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>> {
        match self {
            Self::AccessLog => {
                // Disabling physical optimizer rules to avoid sorts /
                // repartitions (since RepartitionExec / SortExec also
                // has a memory budget which we'll likely hit first)
                Some(vec![])
            }
            Self::AccessLogStreaming => {
                // Disable all physical optimizer rules except the
                // JoinSelection rule to avoid sorts or repartition,
                // as they also have memory budgets that may be hit
                // first
                Some(vec![Arc::new(JoinSelection::new())])
            }
            Self::DictionaryStrings { .. } => {
                // Use default rules
                None
            }
        }
    }
}

fn access_log_batches() -> Vec<RecordBatch> {
    AccessLogGenerator::new()
        .with_row_limit(1000)
        .with_max_batch_size(50)
        .collect()
}

/// If `one_row_batches` is true, then returns new record batches that
/// are one row in size
fn maybe_split_batches(
    batches: Vec<RecordBatch>,
    one_row_batches: bool,
) -> Vec<RecordBatch> {
    if !one_row_batches {
        return batches;
    }

    batches
        .into_iter()
        .flat_map(|mut batch| {
            let mut batches = vec![];
            while batch.num_rows() > 1 {
                batches.push(batch.slice(0, 1));
                batch = batch.slice(1, batch.num_rows() - 1);
            }
            batches
        })
        .collect()
}

static DICT_BATCHES: OnceLock<Vec<RecordBatch>> = OnceLock::new();

/// Returns 5 sorted string dictionary batches each with 50 rows with
/// this schema.
///
/// a: Dictionary<Utf8, Int32>,
/// b: Dictionary<Utf8, Int32>,
fn dict_batches() -> Vec<RecordBatch> {
    DICT_BATCHES.get_or_init(make_dict_batches).clone()
}

fn make_dict_batches() -> Vec<RecordBatch> {
    let batch_size = 50;

    let mut i = 0;
    let gen = std::iter::from_fn(move || {
        // create values like
        // 0000000001
        // 0000000002
        // ...
        // 0000000002

        let values: Vec<_> = (i..i + batch_size)
            .map(|x| format!("{:010}", x / 16))
            .collect();
        //println!("values: \n{values:?}");
        let array: DictionaryArray<Int32Type> =
            values.iter().map(|s| s.as_str()).collect();
        let array = Arc::new(array) as ArrayRef;
        let batch =
            RecordBatch::try_from_iter(vec![("a", array.clone()), ("b", array)]).unwrap();

        i += batch_size;
        Some(batch)
    });

    let num_batches = 5;

    let batches: Vec<_> = gen.take(num_batches).collect();

    batches.iter().enumerate().for_each(|(i, batch)| {
        println!("Dict batch[{i}] size is: {}", batch.get_array_memory_size());
    });

    batches
}

// How many bytes does the memory from dict_batches consume?
fn batches_byte_size(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.get_array_memory_size()).sum()
}

struct DummyStreamPartition {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl PartitionStream for DummyStreamPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // We create an iterator from the record batches and map them into Ok values,
        // converting the iterator into a futures::stream::Stream
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(self.batches.clone()).map(Ok),
        ))
    }
}

///  Wrapper over a TableProvider that can provide ordering information
struct SortedTableProvider {
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
    sort_information: Vec<LexOrdering>,
}

impl SortedTableProvider {
    fn new(batches: Vec<Vec<RecordBatch>>, sort_information: Vec<LexOrdering>) -> Self {
        let schema = batches[0][0].schema();
        Self {
            schema,
            batches,
            sort_information,
        }
    }
}

#[async_trait]
impl TableProvider for SortedTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(self.schema.clone().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mem_exec =
            MemoryExec::try_new(&self.batches, self.schema.clone(), projection.cloned())?
                .with_sort_information(self.sort_information.clone());

        Ok(Arc::new(mem_exec))
    }
}
