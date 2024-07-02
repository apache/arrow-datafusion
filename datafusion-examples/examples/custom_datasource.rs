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

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{UInt64Builder, UInt8Builder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{provider_as_source, TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    project_schema, DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::*;
use datafusion_expr::LogicalPlanBuilder;
use datafusion_physical_expr::EquivalenceProperties;

use async_trait::async_trait;
use tokio::time::timeout;
use datafusion_common::logical_type::field::LogicalField;
use datafusion_common::logical_type::schema::{LogicalSchema, LogicalSchemaRef};

/// This example demonstrates executing a simple query against a custom datasource
#[tokio::main]
async fn main() -> Result<()> {
    // create our custom datasource and adding some users
    let db = CustomDataSource::default();
    db.populate_users();

    search_accounts(db.clone(), None, 3).await?;
    search_accounts(db.clone(), Some(col("bank_account").gt(lit(8000u64))), 1).await?;
    search_accounts(db.clone(), Some(col("bank_account").gt(lit(200u64))), 2).await?;

    Ok(())
}

async fn search_accounts(
    db: CustomDataSource,
    filter: Option<Expr>,
    expected_result_length: usize,
) -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // create logical plan composed of a single TableScan
    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "accounts",
        provider_as_source(Arc::new(db)),
        None,
        vec![],
    )?
    .build()?;

    let mut dataframe = DataFrame::new(ctx.state(), logical_plan)
        .select_columns(&["id", "bank_account"])?;

    if let Some(f) = filter {
        dataframe = dataframe.filter(f)?;
    }

    timeout(Duration::from_secs(10), async move {
        let result = dataframe.collect().await.unwrap();
        let record_batch = result.first().unwrap();

        assert_eq!(expected_result_length, record_batch.column(1).len());
        dbg!(record_batch.columns());
    })
    .await
    .unwrap();

    Ok(())
}

/// A User, with an id and a bank account
#[derive(Clone, Debug)]
struct User {
    id: u8,
    bank_account: u64,
}

/// A custom datasource, used to represent a datastore with a single index
#[derive(Clone)]
pub struct CustomDataSource {
    inner: Arc<Mutex<CustomDataSourceInner>>,
}

struct CustomDataSourceInner {
    data: HashMap<u8, User>,
    bank_account_index: BTreeMap<u64, u8>,
}

impl Debug for CustomDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("custom_db")
    }
}

impl CustomDataSource {
    pub(crate) async fn create_physical_plan(
        &self,
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExec::new(projections, schema, self.clone())))
    }

    pub(crate) fn populate_users(&self) {
        self.add_user(User {
            id: 1,
            bank_account: 9_000,
        });
        self.add_user(User {
            id: 2,
            bank_account: 100,
        });
        self.add_user(User {
            id: 3,
            bank_account: 1_000,
        });
    }

    fn add_user(&self, user: User) {
        let mut inner = self.inner.lock().unwrap();
        inner.bank_account_index.insert(user.bank_account, user.id);
        inner.data.insert(user.id, user);
    }
}

impl Default for CustomDataSource {
    fn default() -> Self {
        CustomDataSource {
            inner: Arc::new(Mutex::new(CustomDataSourceInner {
                data: Default::default(),
                bank_account_index: Default::default(),
            })),
        }
    }
}

#[async_trait]
impl TableProvider for CustomDataSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        LogicalSchemaRef::new(LogicalSchema::new(vec![
            LogicalField::new("id", DataType::UInt8, false),
            LogicalField::new("bank_account", DataType::UInt64, true),
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = SchemaRef::new(self.schema().as_ref().clone().into());
        return self.create_physical_plan(projection, schema).await;
    }
}

#[derive(Debug, Clone)]
struct CustomExec {
    db: CustomDataSource,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl CustomExec {
    fn new(
        projections: Option<&Vec<usize>>,
        schema: SchemaRef,
        db: CustomDataSource,
    ) -> Self {
        let projected_schema = project_schema(&schema, projections).unwrap();
        let cache = Self::compute_properties(projected_schema.clone());
        Self {
            db,
            projected_schema,
            cache,
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
    }
}

impl DisplayAs for CustomExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "CustomExec")
    }
}

impl ExecutionPlan for CustomExec {
    fn name(&self) -> &'static str {
        "CustomExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let users: Vec<User> = {
            let db = self.db.inner.lock().unwrap();
            db.data.values().cloned().collect()
        };

        let mut id_array = UInt8Builder::with_capacity(users.len());
        let mut account_array = UInt64Builder::with_capacity(users.len());

        for user in users {
            id_array.append_value(user.id);
            account_array.append_value(user.bank_account);
        }

        Ok(Box::pin(MemoryStream::try_new(
            vec![RecordBatch::try_new(
                self.projected_schema.clone(),
                vec![
                    Arc::new(id_array.finish()),
                    Arc::new(account_array.finish()),
                ],
            )?],
            self.schema(),
            None,
        )?))
    }
}
