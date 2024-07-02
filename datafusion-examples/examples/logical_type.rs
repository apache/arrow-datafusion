use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::logical_type::schema::{LogicalSchema, LogicalSchemaRef};
use datafusion::error::Result;
use datafusion_expr::{Expr, TableType};
use std::any::Any;
use std::sync::Arc;
use arrow::util::pretty::pretty_format_batches;
use arrow_schema::{DataType, Field, TimeUnit};
use datafusion::prelude::SessionContext;
use datafusion_common::logical_type::{ExtensionType};
use datafusion_common::logical_type::field::LogicalField;
use datafusion_common::logical_type::signature::LogicalType;

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table("example", Arc::new(ExampleTableSource::default()))?;

    let df = ctx.sql("SELECT * FROM example").await?;
    let records = df.collect().await?;

    println!("{}", pretty_format_batches(&records)?);

    Ok(())
}

#[derive(Debug)]
struct CustomMagicalType {
    logical: LogicalType,
    physical: DataType
}

impl Default for CustomMagicalType {
    fn default() -> Self {
        Self {
            logical: LogicalType::Utf8,
            physical: DataType::new_list(DataType::UInt8, false),
        }
    }
}

impl ExtensionType for CustomMagicalType {
    fn logical(&self) -> &LogicalType {
        &self.logical
    }

    fn physical(&self) -> &DataType {
        &self.physical
    }

    // TODO: materialisation methods?
}

#[derive(Default)]
struct ExampleTableSource {}

#[async_trait::async_trait]
impl TableProvider for ExampleTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> LogicalSchemaRef {
        // TODO: ugly?
        let custom_magical_type: Arc<dyn ExtensionType + Send + Sync> = Arc::new(CustomMagicalType::default());

        // This schema will be equivalent to:
        // a -> Timestamp(Microsecond, None)
        // b -> Utf8
        // c -> Int64
        Arc::new(LogicalSchema::new(vec![
            LogicalField::new(
                "a",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int64, false)),
                    Arc::new(Field::new("values", DataType::Timestamp(TimeUnit::Microsecond, None), false))
                ),
                false
            ),
            LogicalField::new(
                "b",
                custom_magical_type,
                false
            ),
            LogicalField::new(
                "c",
                DataType::Int64,
                true,
            )
        ]))
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
