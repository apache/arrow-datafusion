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

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::logical_type::LogicalType;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_expr::WindowUDF;
use datafusion_expr::{
    logical_plan::builder::LogicalTableSource, AggregateUDF, ScalarUDF, TableSource,
};
use datafusion_sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{dialect::GenericDialect, parser::Parser},
    TableReference,
};
use std::{collections::HashMap, sync::Arc};

fn main() {
    let sql = "SELECT \
            c.id, c.first_name, c.last_name, \
            COUNT(*) as num_orders, \
            SUM(o.price) AS total_price, \
            SUM(o.price * s.sales_tax) AS state_tax \
        FROM customer c \
        JOIN state s ON c.state = s.id \
        JOIN orders o ON c.id = o.customer_id \
        WHERE o.price > 0 \
        AND c.last_name LIKE 'G%' \
        GROUP BY 1, 2, 3 \
        ORDER BY state_tax DESC";

    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // show the plan
    println!("{plan:?}");
}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
}

impl MyContextProvider {
    fn new() -> Self {
        let mut tables = HashMap::new();
        tables.insert(
            "customer".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("state", DataType::Utf8, false),
            ]),
        );
        tables.insert(
            "state".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("sales_tax", DataType::Decimal128(10, 2), false),
            ]),
        );
        tables.insert(
            "orders".to_string(),
            create_table_source(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("customer_id", DataType::Int32, false),
                Field::new("item_id", DataType::Int32, false),
                Field::new("quantity", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ]),
        );
        Self {
            tables,
            options: Default::default(),
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(table.clone()),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<LogicalType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}
