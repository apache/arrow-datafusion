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

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::logical_expr::builder::LogicalTableSource;
use datafusion::logical_expr::{Filter, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::prelude::*;
use std::sync::Arc;

#[test]
fn create_plan() -> Result<()> {
    //begin:create_plan
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // create a TableScan plan
    let projection = None; // optional projection
    let filters = vec![]; // optional filters to push down
    let fetch = None; // optional LIMIT
    let table_scan = LogicalPlan::TableScan(TableScan::try_new(
        "person",
        Arc::new(table_source),
        projection,
        filters,
        fetch,
    )?);

    // create a Filter plan that evaluates `id > 500` and wraps the TableScan
    let filter_expr = col("id").gt(lit(500));
    let plan = LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(table_scan))?);

    // print the plan
    println!("{}", plan.display_indent_schema());
    //end:create_plan

    assert_eq!(
        plan.display_indent_schema().to_string(),
        r#"Filter: id > Int32(500) [id:Int32;N, name:Utf8;N]
  TableScan: person [id:Int32;N, name:Utf8;N]"#
    );

    Ok(())
}

#[test]
fn build_plan() -> Result<()> {
    //begin:build_plan
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // optional projection
    let projection = None;

    // create a LogicalPlanBuilder for a table scan
    let builder = LogicalPlanBuilder::scan("person", Arc::new(table_source), projection)?;

    // perform a filter that evaluates `id > 500`, and build the plan
    let plan = builder.filter(col("id").gt(lit(500)))?.build()?;

    // print the plan
    println!("{}", plan.display_indent_schema());
    //end:build_plan

    assert_eq!(
        plan.display_indent_schema().to_string(),
        r#"Filter: person.id > Int32(500) [id:Int32;N, name:Utf8;N]
  TableScan: person [id:Int32;N, name:Utf8;N]"#
    );

    Ok(())
}
