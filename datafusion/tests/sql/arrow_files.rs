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

use datafusion::execution::options::ArrowReadOptions;

use super::*;

async fn register_arrow(ctx: &mut ExecutionContext) {
    ctx.register_arrow(
        "arrow_simple",
        "tests/example.arrow",
        ArrowReadOptions::default(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn arrow_query() {
    let mut ctx = ExecutionContext::new();
    register_arrow(&mut ctx).await;
    let sql = "SELECT * FROM arrow_simple";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+-----+-------+",
        "| f0 | f1  | f2    |",
        "+----+-----+-------+",
        "| 1  | foo | true  |",
        "| 2  | bar |       |",
        "| 3  | baz | false |",
        "| 4  |     | true  |",
        "+----+-----+-------+",
    ];

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn arrow_explain() {
    let mut ctx = ExecutionContext::new();
    register_arrow(&mut ctx).await;
    let sql = "EXPLAIN SELECT * FROM arrow_simple";
    let actual = execute(&mut ctx, sql).await;
    let actual = normalize_vec_for_explain(actual);
    let expected = vec![
        vec![
            "logical_plan",
            "Projection: #arrow_simple.f0, #arrow_simple.f1, #arrow_simple.f2\
            \n  TableScan: arrow_simple projection=Some([0, 1, 2])",
        ],
        vec![
            "physical_plan",
            "ProjectionExec: expr=[f0@0 as f0, f1@1 as f1, f2@2 as f2]\
        \n  ArrowExec: limit=None, partitions=[tests/example.arrow]\n",
        ],
    ];

    assert_eq!(expected, actual);
}
