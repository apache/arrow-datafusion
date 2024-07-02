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

use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::test_util::TestTableFactory;
use datafusion_common::logical_type::ExtensionType;
use super::*;

#[tokio::test]
async fn create_custom_table() -> Result<()> {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::new_with_config_rt(ses, Arc::new(env));
    state
        .table_factories_mut()
        .insert("DELTATABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();
    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl() -> Result<()> {
    let cfg = RuntimeConfig::new();
    let env = RuntimeEnv::new(cfg).unwrap();
    let ses = SessionConfig::new();
    let mut state = SessionState::new_with_config_rt(ses, Arc::new(env));
    state
        .table_factories_mut()
        .insert("MOCKTABLE".to_string(), Arc::new(TestTableFactory {}));
    let ctx = SessionContext::new_with_state(state);

    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS MOCKTABLE LOCATION 'mockprotocol://path/to/table';";
    ctx.sql(sql).await.unwrap();

    let cat = ctx.catalog("datafusion").unwrap();
    let schema = cat.schema("public").unwrap();

    let exists = schema.table_exist("dt");
    assert!(exists, "Table should have been created!");

    let table_schema = schema.table("dt").await.unwrap().unwrap().schema();

    assert_eq!(3, table_schema.fields().len());

    assert_eq!(&DataType::Int32, table_schema.field(0).data_type().physical());
    assert_eq!(&DataType::Utf8, table_schema.field(1).data_type().physical());
    assert_eq!(&DataType::Boolean, table_schema.field(2).data_type().physical());

    Ok(())
}
