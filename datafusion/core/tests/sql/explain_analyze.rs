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

use super::*;

use datafusion::config::ConfigOptions;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::metrics::Timestamp;

#[tokio::test]
async fn explain_analyze_baseline_metrics() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    // and then validate the presence of baseline metrics for supported operators
    let config = SessionConfig::new()
        .with_target_partitions(3)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv_by_sql(&ctx).await;
    // a query with as many operators as we have metrics for
    let sql = "EXPLAIN ANALYZE \
               SELECT count(*) as cnt FROM \
                 (SELECT count(*), c1 \
                  FROM aggregate_test_100 \
                  WHERE c13 != 'C2GT5KVyOPZpgKVl110TyZO0NcJ434' \
                  GROUP BY c1 \
                  ORDER BY c1 ) AS a \
                 UNION ALL \
               SELECT 1 as cnt \
                 UNION ALL \
               SELECT lead(c1, 1) OVER () as cnt FROM (select 1 as c1) AS b \
               LIMIT 3";
    println!("running query: {sql}");
    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let results = collect(physical_plan.clone(), task_ctx).await.unwrap();
    let formatted = arrow::util::pretty::pretty_format_batches(&results)
        .unwrap()
        .to_string();
    println!("Query Output:\n\n{formatted}");

    assert_metrics!(
        &formatted,
        "AggregateExec: mode=Partial, gby=[]",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1]",
        "metrics=[output_rows=5, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "FilterExec: c13@1 != C2GT5KVyOPZpgKVl110TyZO0NcJ434",
        "metrics=[output_rows=99, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "GlobalLimitExec: skip=0, fetch=3, ",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "LocalLimitExec: fetch=3",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "ProjectionExec: expr=[COUNT(*)",
        "metrics=[output_rows=1, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "CoalesceBatchesExec: target_batch_size=4096",
        "metrics=[output_rows=5, elapsed_compute"
    );
    assert_metrics!(
        &formatted,
        "UnionExec",
        "metrics=[output_rows=3, elapsed_compute="
    );
    assert_metrics!(
        &formatted,
        "WindowAggExec",
        "metrics=[output_rows=1, elapsed_compute="
    );

    fn expected_to_have_metrics(plan: &dyn ExecutionPlan) -> bool {
        use datafusion::physical_plan;
        use datafusion::physical_plan::sorts;

        plan.as_any().downcast_ref::<sorts::sort::SortExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::aggregates::AggregateExec>().is_some()
            // CoalescePartitionsExec doesn't do any work so is not included
            || plan.as_any().downcast_ref::<physical_plan::filter::FilterExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::GlobalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::limit::LocalLimitExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::projection::ProjectionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_batches::CoalesceBatchesExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::coalesce_partitions::CoalescePartitionsExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::union::UnionExec>().is_some()
            || plan.as_any().downcast_ref::<physical_plan::windows::WindowAggExec>().is_some()
    }

    // Validate that the recorded elapsed compute time was more than
    // zero for all operators as well as the start/end timestamp are set
    struct TimeValidator {}
    impl ExecutionPlanVisitor for TimeValidator {
        type Error = std::convert::Infallible;

        fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
            if !expected_to_have_metrics(plan) {
                return Ok(true);
            }
            let metrics = plan.metrics().unwrap().aggregate_by_name();

            assert!(
                metrics.output_rows().unwrap() > 0,
                "plan: {}",
                DisplayableExecutionPlan::with_metrics(plan).one_line()
            );
            assert!(
                metrics.elapsed_compute().unwrap() > 0,
                "plan: {}",
                DisplayableExecutionPlan::with_metrics(plan).one_line()
            );

            let mut saw_start = false;
            let mut saw_end = false;
            metrics.iter().for_each(|m| match m.value() {
                MetricValue::StartTimestamp(ts) => {
                    saw_start = true;
                    assert!(nanos_from_timestamp(ts) > 0);
                }
                MetricValue::EndTimestamp(ts) => {
                    saw_end = true;
                    assert!(nanos_from_timestamp(ts) > 0);
                }
                _ => {}
            });

            assert!(saw_start);
            assert!(saw_end);

            Ok(true)
        }
    }

    datafusion::physical_plan::accept(physical_plan.as_ref(), &mut TimeValidator {})
        .unwrap();
}
fn nanos_from_timestamp(ts: &Timestamp) -> i64 {
    ts.value().unwrap().timestamp_nanos_opt().unwrap()
}
#[tokio::test]
async fn csv_explain_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let logical_schema = dataframe.schema();
    let plan = dataframe.logical_plan();

    //
    println!("SQL: {sql}");
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: aggregate_test_100.c1",
        "    Filter: aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan,",
        "// display it online here: https://dreampuf.github.io/GraphvizOnline",
        "",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // Optimized logical plan
    let state = ctx.state();
    let msg = format!("Optimizing logical plan for '{sql}': {plan:?}");
    let plan = state.optimize(plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(logical_schema, optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Filter: aggregate_test_100.c2 > Int8(10), projection=[c1] [c1:Utf8]",
        "    TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)] [c1:Utf8, c2:Int8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]",
        "    TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan,",
        "// display it online here: https://dreampuf.github.io/GraphvizOnline",
        "",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_5",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    6[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    7[shape=box label=\"Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]\\nSchema: [c1:Utf8]\"]",
        "    6 -> 7 [arrowhead=none, arrowtail=normal, dir=back]",
        "    8[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{sql}': {plan:?}");
    let plan = state.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{sql}': {plan:?}");
    let results = collect(plan, state.task_ctx()).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally dependant (e.g. full path of the test file), only verify important content
    assert_contains!(&actual, "logical_plan");
    assert_contains!(
        actual,
        "Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]"
    );
}

#[tokio::test]
async fn csv_explain_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";
    let actual = execute(&ctx, sql).await;

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // Don't actually test the contents of the debuging output (as
    // that may change and keeping this test updated will be a
    // pain). Instead just check for a few key pieces.
    assert_contains!(&actual, "logical_plan");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "aggregate_test_100.c2 > Int64(10)");

    // ensure the "same text as above" optimization is working
    assert_contains!(actual, "SAME TEXT AS ABOVE");
}

#[tokio::test]
async fn csv_explain_inlist_verbose() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 in (1,2,4)";
    let actual = execute(&ctx, sql).await;

    // Optimized by PreCastLitInComparisonExpressions rule
    // the data type of c2 is INT8, the type of `1,2,4` is INT64.
    // the value of `1,2,4` will be casted to INT8 and pre-calculated

    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();

    // before optimization (Int64 literals)
    assert_contains!(
        &actual,
        "aggregate_test_100.c2 IN ([Int64(1), Int64(2), Int64(4)])"
    );
    // after optimization (casted to Int8)
    assert_contains!(
        &actual,
        "aggregate_test_100.c2 IN ([Int8(1), Int8(2), Int8(4)])"
    );
}

#[tokio::test]
async fn csv_explain_verbose_plans() {
    // This test verify the look of each plan in its full cycle plan creation

    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN VERBOSE SELECT c1 FROM aggregate_test_100 where c2 > 10";

    // Logical plan
    // Create plan
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let logical_schema = dataframe.schema().clone();
    //
    println!("SQL: {sql}");

    //
    // Verify schema
    let expected: Vec<&str> = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Projection: aggregate_test_100.c1 [c1:Utf8]",
        "    Filter: aggregate_test_100.c2 > Int64(10) [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
        "      TableScan: aggregate_test_100 [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]",
    ];
    let formatted = dataframe.logical_plan().display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Projection: aggregate_test_100.c1",
        "    Filter: aggregate_test_100.c2 > Int64(10)",
        "      TableScan: aggregate_test_100",
    ];
    let formatted = dataframe.logical_plan().display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan,",
        "// display it online here: https://dreampuf.github.io/GraphvizOnline",
        "",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Projection: aggregate_test_100.c1\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"Filter: aggregate_test_100.c2 > Int64(10)\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "    5[shape=box label=\"TableScan: aggregate_test_100\"]",
        "    4 -> 5 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_6",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    7[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    8[shape=box label=\"Projection: aggregate_test_100.c1\\nSchema: [c1:Utf8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "    9[shape=box label=\"Filter: aggregate_test_100.c2 > Int64(10)\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    8 -> 9 [arrowhead=none, arrowtail=normal, dir=back]",
        "    10[shape=box label=\"TableScan: aggregate_test_100\\nSchema: [c1:Utf8, c2:Int8, c3:Int16, c4:Int16, c5:Int32, c6:Int64, c7:Int16, c8:Int32, c9:UInt32, c10:UInt64, c11:Float32, c12:Float64, c13:Utf8]\"]",
        "    9 -> 10 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = dataframe.logical_plan().display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // Optimized logical plan
    let msg = format!("Optimizing logical plan for '{sql}': {dataframe:?}");
    let (state, plan) = dataframe.into_parts();
    let plan = state.optimize(&plan).expect(&msg);
    let optimized_logical_schema = plan.schema();
    // Both schema has to be the same
    assert_eq!(&logical_schema, optimized_logical_schema.as_ref());
    //
    // Verify schema
    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Filter: aggregate_test_100.c2 > Int8(10), projection=[c1] [c1:Utf8]",
        "    TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)] [c1:Utf8, c2:Int8]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // Verify the text format of the plan
    let expected = vec![
        "Explain",
        "  Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]",
        "    TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]",
    ];
    let formatted = plan.display_indent().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );
    //
    // verify the grahviz format of the plan
    let expected = vec![
        "// Begin DataFusion GraphViz Plan,",
        "// display it online here: https://dreampuf.github.io/GraphvizOnline",
        "",
        "digraph {",
        "  subgraph cluster_1",
        "  {",
        "    graph[label=\"LogicalPlan\"]",
        "    2[shape=box label=\"Explain\"]",
        "    3[shape=box label=\"Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]\"]",
        "    2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]",
        "    4[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\"]",
        "    3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "  subgraph cluster_5",
        "  {",
        "    graph[label=\"Detailed LogicalPlan\"]",
        "    6[shape=box label=\"Explain\\nSchema: [plan_type:Utf8, plan:Utf8]\"]",
        "    7[shape=box label=\"Filter: aggregate_test_100.c2 > Int8(10), projection=[c1]\\nSchema: [c1:Utf8]\"]",
        "    6 -> 7 [arrowhead=none, arrowtail=normal, dir=back]",
        "    8[shape=box label=\"TableScan: aggregate_test_100 projection=[c1, c2], partial_filters=[aggregate_test_100.c2 > Int8(10)]\\nSchema: [c1:Utf8, c2:Int8]\"]",
        "    7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]",
        "  }",
        "}",
        "// End DataFusion GraphViz Plan",
    ];
    let formatted = plan.display_graphviz().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    // Physical plan
    // Create plan
    let msg = format!("Creating physical plan for '{sql}': {plan:?}");
    let plan = state.create_physical_plan(&plan).await.expect(&msg);
    //
    // Execute plan
    let msg = format!("Executing physical plan for '{sql}': {plan:?}");
    let task_ctx = ctx.task_ctx();
    let results = collect(plan, task_ctx).await.expect(&msg);
    let actual = result_vec(&results);
    // flatten to a single string
    let actual = actual.into_iter().map(|r| r.join("\t")).collect::<String>();
    // Since the plan contains path that are environmentally
    // dependant(e.g. full path of the test file), only verify
    // important content
    assert_contains!(&actual, "logical_plan after optimize_projections");
    assert_contains!(&actual, "physical_plan");
    assert_contains!(&actual, "FilterExec: c2@1 > 10");
}

#[tokio::test]
async fn explain_analyze_runs_optimizers() {
    // repro for https://github.com/apache/arrow-datafusion/issues/917
    // where EXPLAIN ANALYZE was not correctly running optiimizer
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    // This happens as an optimization pass where count(*) can be
    // answered using statistics only.
    let expected = "EmptyExec: produce_one_row=true";

    let sql = "EXPLAIN SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);

    // EXPLAIN ANALYZE should work the same
    let sql = "EXPLAIN  ANALYZE SELECT count(*) from alltypes_plain";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    assert_contains!(actual, expected);
}

#[tokio::test]
async fn test_physical_plan_display_indent() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new()
        .with_target_partitions(9000)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1, MAX(c12), MIN(c12) as the_min \
               FROM aggregate_test_100 \
               WHERE c12 < 10 \
               GROUP BY c1 \
               ORDER BY the_min DESC \
               LIMIT 10";
    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();
    let expected = vec![
        "GlobalLimitExec: skip=0, fetch=10",
        "  SortPreservingMergeExec: [the_min@2 DESC], fetch=10",
        "    SortExec: TopK(fetch=10), expr=[the_min@2 DESC]",
        "      ProjectionExec: expr=[c1@0 as c1, MAX(aggregate_test_100.c12)@1 as MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)@2 as the_min]",
        "        AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "          CoalesceBatchesExec: target_batch_size=4096",
        "            RepartitionExec: partitioning=Hash([c1@0], 9000), input_partitions=9000",
        "              AggregateExec: mode=Partial, gby=[c1@0 as c1], aggr=[MAX(aggregate_test_100.c12), MIN(aggregate_test_100.c12)]",
        "                CoalesceBatchesExec: target_batch_size=4096",
        "                  FilterExec: c12@1 < 10",
        "                    RepartitionExec: partitioning=RoundRobinBatch(9000), input_partitions=1",
        "                      CsvExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1, c12], has_header=true",
    ];

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent(true))
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>();
    assert_eq!(
        expected, actual,
        "expected:\n{expected:#?}\nactual:\n\n{actual:#?}\n"
    );
}

#[tokio::test]
async fn test_physical_plan_display_indent_multi_children() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new()
        .with_target_partitions(9000)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    // ensure indenting works for nodes with multiple children
    register_aggregate_csv(&ctx).await.unwrap();
    let sql = "SELECT c1 \
               FROM (select c1 from aggregate_test_100) AS a \
               JOIN\
               (select c1 as c2 from aggregate_test_100) AS b \
               ON c1=c2\
               ";

    let dataframe = ctx.sql(sql).await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();
    let expected = vec![
        "ProjectionExec: expr=[c1@0 as c1]",
        "  CoalesceBatchesExec: target_batch_size=4096",
        "    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(c1@0, c2@0)]",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([c1@0], 9000), input_partitions=9000",
        "          RepartitionExec: partitioning=RoundRobinBatch(9000), input_partitions=1",
        "            CsvExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1], has_header=true",
        "      CoalesceBatchesExec: target_batch_size=4096",
        "        RepartitionExec: partitioning=Hash([c2@0], 9000), input_partitions=9000",
        "          RepartitionExec: partitioning=RoundRobinBatch(9000), input_partitions=1",
        "            ProjectionExec: expr=[c1@0 as c2]",
        "              CsvExec: file_groups={1 group: [[ARROW_TEST_DATA/csv/aggregate_test_100.csv]]}, projection=[c1], has_header=true",
    ];

    let normalizer = ExplainNormalizer::new();
    let actual = format!("{}", displayable(physical_plan.as_ref()).indent(true))
        .trim()
        .lines()
        // normalize paths
        .map(|s| normalizer.normalize(s))
        .collect::<Vec<_>>();

    assert_eq!(
        expected, actual,
        "expected:\n{expected:#?}\nactual:\n\n{actual:#?}\n"
    );
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze() {
    // This test uses the execute function to run an actual plan under EXPLAIN ANALYZE
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN ANALYZE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // Only test basic plumbing and try to avoid having to change too
    // many things. explain_analyze_baseline_metrics covers the values
    // in greater depth
    let needle = "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[COUNT(*)], metrics=[output_rows=5";
    assert_contains!(&formatted, needle);

    let verbose_needle = "Output Rows";
    assert_not_contains!(formatted, verbose_needle);
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze_order_by() {
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql = "EXPLAIN ANALYZE SELECT c1 FROM aggregate_test_100 order by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // Ensure that the ordering is not optimized away from the plan
    // https://github.com/apache/arrow-datafusion/issues/6379
    let needle =
        "SortExec: expr=[c1@0 ASC NULLS LAST], metrics=[output_rows=100, elapsed_compute";
    assert_contains!(&formatted, needle);
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn parquet_explain_analyze() {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    let sql = "EXPLAIN ANALYZE select id, float_col, timestamp_col from alltypes_plain where timestamp_col > to_timestamp('2009-02-01T00:00:00'); ";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain aggregated stats
    assert_contains!(&formatted, "output_rows=8");
    assert_contains!(&formatted, "row_groups_pruned=0");
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn parquet_explain_analyze_verbose() {
    let ctx = SessionContext::new();
    register_alltypes_parquet(&ctx).await;

    let sql = "EXPLAIN ANALYZE VERBOSE select id, float_col, timestamp_col from alltypes_plain where timestamp_col > to_timestamp('2009-02-01T00:00:00'); ";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain the raw per file stats (with the label)
    assert_contains!(&formatted, "row_groups_pruned{partition=0");
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn csv_explain_analyze_verbose() {
    // This test uses the execute function to run an actual plan under EXPLAIN VERBOSE ANALYZE
    let ctx = SessionContext::new();
    register_aggregate_csv_by_sql(&ctx).await;
    let sql =
        "EXPLAIN ANALYZE VERBOSE SELECT count(*), c1 FROM aggregate_test_100 group by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    let verbose_needle = "Output Rows";
    assert_contains!(formatted, verbose_needle);
}

#[tokio::test]
async fn explain_logical_plan_only() {
    let mut config = ConfigOptions::new();
    config.explain.logical_plan_only = true;
    let ctx = SessionContext::new_with_config(config.into());
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    let expected = vec![
        vec![
            "logical_plan",
            "Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1)) AS COUNT(*)]]\
            \n  SubqueryAlias: t\
            \n    Projection: \
            \n      Values: (Utf8(\"a\"), Int64(1), Int64(100)), (Utf8(\"a\"), Int64(2), Int64(150))"
        ]];
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn explain_physical_plan_only() {
    let mut config = ConfigOptions::new();
    config.explain.physical_plan_only = true;
    let ctx = SessionContext::new_with_config(config.into());
    let sql = "EXPLAIN select count(*) from (values ('a', 1, 100), ('a', 2, 150)) as t (c1,c2,c3)";
    let actual = execute(&ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);

    let expected = vec![vec![
        "physical_plan",
        "ProjectionExec: expr=[2 as COUNT(*)]\
        \n  EmptyExec: produce_one_row=true\
        \n",
    ]];
    assert_eq!(expected, actual);
}

#[tokio::test]
async fn csv_explain_analyze_with_statistics() {
    let mut config = ConfigOptions::new();
    config.explain.physical_plan_only = true;
    config.explain.show_statistics = true;
    let ctx = SessionContext::new_with_config(config.into());
    register_aggregate_csv_by_sql(&ctx).await;

    let sql = "EXPLAIN ANALYZE SELECT c1 FROM aggregate_test_100";
    let actual = execute_to_batches(&ctx, sql).await;
    let formatted = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();

    // should contain scan statistics
    assert_contains!(
        &formatted,
        ", statistics=[Rows=Absent, Bytes=Absent, [(Col[0]:)]]"
    );
}
