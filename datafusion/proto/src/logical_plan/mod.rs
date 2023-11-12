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

use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use crate::common::{byte_to_string, proto_error, str_to_byte};
use crate::protobuf::logical_plan_node::LogicalPlanType::CustomScan;
use crate::protobuf::{CustomTableScanNode, LogicalExprNodeCollection};
use crate::{
    convert_required,
    protobuf::{
        self, listing_table_scan_node::FileFormatType,
        logical_plan_node::LogicalPlanType, LogicalExtensionNode, LogicalPlanNode,
    },
};

use arrow::datatypes::{DataType, Schema, SchemaRef};
#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::{
    datasource::{
        file_format::{avro::AvroFormat, csv::CsvFormat, FileFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        view::ViewTable,
        TableProvider,
    },
    datasource::{provider_as_source, source_as_provider},
    prelude::SessionContext,
};
use datafusion_common::logical_type::LogicalType;
use datafusion_common::plan_datafusion_err;
use datafusion_common::{
    context, internal_err, not_impl_err, parsers::CompressionTypeVariant,
    DataFusionError, OwnedTableReference, Result,
};
use datafusion_expr::{
    logical_plan::{
        builder::project, Aggregate, CreateCatalog, CreateCatalogSchema,
        CreateExternalTable, CreateView, CrossJoin, DdlStatement, Distinct,
        EmptyRelation, Extension, Join, JoinConstraint, Limit, Prepare, Projection,
        Repartition, Sort, SubqueryAlias, TableScan, Values, Window,
    },
    DropView, Expr, LogicalPlan, LogicalPlanBuilder,
};

use prost::bytes::BufMut;
use prost::Message;

pub mod from_proto;
pub mod to_proto;

impl From<from_proto::Error> for DataFusionError {
    fn from(e: from_proto::Error) -> Self {
        plan_datafusion_err!("{}", e)
    }
}

impl From<to_proto::Error> for DataFusionError {
    fn from(e: to_proto::Error) -> Self {
        plan_datafusion_err!("{}", e)
    }
}

pub trait AsLogicalPlan: Debug + Send + Sync + Clone {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized;

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized;

    fn try_into_logical_plan(
        &self,
        ctx: &SessionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan>;

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized;
}

pub trait LogicalExtensionCodec: Debug + Send + Sync {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[LogicalPlan],
        ctx: &SessionContext,
    ) -> Result<Extension>;

    fn try_encode(&self, node: &Extension, buf: &mut Vec<u8>) -> Result<()>;

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        schema: SchemaRef,
        ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>>;

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct DefaultLogicalExtensionCodec {}

impl LogicalExtensionCodec for DefaultLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &SessionContext,
    ) -> Result<Extension> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _schema: SchemaRef,
        _ctx: &SessionContext,
    ) -> Result<Arc<dyn TableProvider>> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }

    fn try_encode_table_provider(
        &self,
        _node: Arc<dyn TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> Result<()> {
        not_impl_err!("LogicalExtensionCodec is not provided")
    }
}

#[macro_export]
macro_rules! into_logical_plan {
    ($PB:expr, $CTX:expr, $CODEC:expr) => {{
        if let Some(field) = $PB.as_ref() {
            field.as_ref().try_into_logical_plan($CTX, $CODEC)
        } else {
            Err(proto_error("Missing required field in protobuf"))
        }
    }};
}

fn from_owned_table_reference(
    table_ref: Option<&protobuf::OwnedTableReference>,
    error_context: &str,
) -> Result<OwnedTableReference> {
    let table_ref = table_ref.ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Protobuf deserialization error, {error_context} was missing required field name."
        ))
    })?;

    Ok(table_ref.clone().try_into()?)
}

impl AsLogicalPlan for LogicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        LogicalPlanNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to decode logical plan: {e:?}"))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<()>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            DataFusionError::Internal(format!("failed to encode logical plan: {e:?}"))
        })
    }

    fn try_into_logical_plan(
        &self,
        ctx: &SessionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan> {
        let plan = self.logical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{self:?}'"
            ))
        })?;
        match plan {
            LogicalPlanType::Values(values) => {
                let n_cols = values.n_cols as usize;
                let values: Vec<Vec<Expr>> = if values.values_list.is_empty() {
                    Ok(Vec::new())
                } else if values.values_list.len() % n_cols != 0 {
                    internal_err!(
                        "Invalid values list length, expect {} to be divisible by {}",
                        values.values_list.len(),
                        n_cols
                    )
                } else {
                    values
                        .values_list
                        .chunks_exact(n_cols)
                        .map(|r| {
                            r.iter()
                                .map(|expr| from_proto::parse_expr(expr, ctx))
                                .collect::<Result<Vec<_>, from_proto::Error>>()
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| e.into())
                }?;
                LogicalPlanBuilder::values(values)?.build()
            }
            LogicalPlanType::Projection(projection) => {
                let input: LogicalPlan =
                    into_logical_plan!(projection.input, ctx, extension_codec)?;
                let expr: Vec<Expr> = projection
                    .expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let new_proj = project(input, expr)?;
                match projection.optional_alias.as_ref() {
                    Some(a) => match a {
                        protobuf::projection_node::OptionalAlias::Alias(alias) => {
                            Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                                new_proj,
                                alias.clone(),
                            )?))
                        }
                    },
                    _ => Ok(new_proj),
                }
            }
            LogicalPlanType::Selection(selection) => {
                let input: LogicalPlan =
                    into_logical_plan!(selection.input, ctx, extension_codec)?;
                let expr: Expr = selection
                    .expr
                    .as_ref()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .transpose()?
                    .ok_or_else(|| {
                        DataFusionError::Internal("expression required".to_string())
                    })?;
                // .try_into()?;
                LogicalPlanBuilder::from(input).filter(expr)?.build()
            }
            LogicalPlanType::Window(window) => {
                let input: LogicalPlan =
                    into_logical_plan!(window.input, ctx, extension_codec)?;
                let window_expr = window
                    .window_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input).window(window_expr)?.build()
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input: LogicalPlan =
                    into_logical_plan!(aggregate.input, ctx, extension_codec)?;
                let group_expr = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                let aggr_expr = aggregate
                    .aggr_expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .aggregate(group_expr, aggr_expr)?
                    .build()
            }
            LogicalPlanType::ListingScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let filters = scan
                    .filters
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let mut all_sort_orders = vec![];
                for order in &scan.file_sort_order {
                    let file_sort_order = order
                        .logical_expr_nodes
                        .iter()
                        .map(|expr| from_proto::parse_expr(expr, ctx))
                        .collect::<Result<Vec<_>, _>>()?;
                    all_sort_orders.push(file_sort_order)
                }

                let file_format: Arc<dyn FileFormat> =
                    match scan.file_format_type.as_ref().ok_or_else(|| {
                        proto_error(format!(
                            "logical_plan::from_proto() Unsupported file format '{self:?}'"
                        ))
                    })? {
                        #[cfg(feature = "parquet")]
                        &FileFormatType::Parquet(protobuf::ParquetFormat {}) => {
                            Arc::new(ParquetFormat::default())
                        }
                        FileFormatType::Csv(protobuf::CsvFormat {
                            has_header,
                            delimiter,
                            quote,
                            optional_escape
                        }) => {
                            let mut csv = CsvFormat::default()
                            .with_has_header(*has_header)
                            .with_delimiter(str_to_byte(delimiter, "delimiter")?)
                            .with_quote(str_to_byte(quote, "quote")?);
                            if let Some(protobuf::csv_format::OptionalEscape::Escape(escape)) = optional_escape {
                                csv = csv.with_quote(str_to_byte(escape, "escape")?);
                            }
                            Arc::new(csv)},
                        FileFormatType::Avro(..) => Arc::new(AvroFormat),
                    };

                let table_paths = &scan
                    .paths
                    .iter()
                    .map(ListingTableUrl::parse)
                    .collect::<Result<Vec<_>, _>>()?;

                let options = ListingOptions::new(file_format)
                    .with_file_extension(&scan.file_extension)
                    .with_table_partition_cols(
                        scan.table_partition_cols
                            .iter()
                            .map(|col| {
                                (
                                    col.clone(),
                                    schema
                                        .field_with_name(col)
                                        .unwrap()
                                        .data_type()
                                        .clone(),
                                )
                            })
                            .collect(),
                    )
                    .with_collect_stat(scan.collect_stat)
                    .with_target_partitions(scan.target_partitions as usize)
                    .with_file_sort_order(all_sort_orders);

                let config =
                    ListingTableConfig::new_with_multi_paths(table_paths.clone())
                        .with_listing_options(options)
                        .with_schema(Arc::new(schema));

                let provider = ListingTable::try_new(config)?.with_cache(
                    ctx.state()
                        .runtime_env()
                        .cache_manager
                        .get_file_statistic_cache(),
                );

                let table_name = from_owned_table_reference(
                    scan.table_name.as_ref(),
                    "ListingTableScan",
                )?;

                LogicalPlanBuilder::scan_with_filters(
                    table_name,
                    provider_as_source(Arc::new(provider)),
                    projection,
                    filters,
                )?
                .build()
            }
            LogicalPlanType::CustomScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;
                let schema = Arc::new(schema);
                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let filters = scan
                    .filters
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let provider = extension_codec.try_decode_table_provider(
                    &scan.custom_table_data,
                    schema,
                    ctx,
                )?;

                let table_name =
                    from_owned_table_reference(scan.table_name.as_ref(), "CustomScan")?;

                LogicalPlanBuilder::scan_with_filters(
                    table_name,
                    provider_as_source(provider),
                    projection,
                    filters,
                )?
                .build()
            }
            LogicalPlanType::Sort(sort) => {
                let input: LogicalPlan =
                    into_logical_plan!(sort.input, ctx, extension_codec)?;
                let sort_expr: Vec<Expr> = sort
                    .expr
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input).sort(sort_expr)?.build()
            }
            LogicalPlanType::Repartition(repartition) => {
                use datafusion::logical_expr::Partitioning;
                let input: LogicalPlan =
                    into_logical_plan!(repartition.input, ctx, extension_codec)?;
                use protobuf::repartition_node::PartitionMethod;
                let pb_partition_method = repartition.partition_method.as_ref().ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, RepartitionNode was missing required field 'partition_method'",
                    ))
                })?;

                let partitioning_scheme = match pb_partition_method {
                    PartitionMethod::Hash(protobuf::HashRepartition {
                        hash_expr: pb_hash_expr,
                        partition_count,
                    }) => Partitioning::Hash(
                        pb_hash_expr
                            .iter()
                            .map(|expr| from_proto::parse_expr(expr, ctx))
                            .collect::<Result<Vec<_>, _>>()?,
                        *partition_count as usize,
                    ),
                    PartitionMethod::RoundRobin(partition_count) => {
                        Partitioning::RoundRobinBatch(*partition_count as usize)
                    }
                };

                LogicalPlanBuilder::from(input)
                    .repartition(partitioning_scheme)?
                    .build()
            }
            LogicalPlanType::EmptyRelation(empty_relation) => {
                LogicalPlanBuilder::empty(empty_relation.produce_one_row).build()
            }
            LogicalPlanType::CreateExternalTable(create_extern_table) => {
                let pb_schema = (create_extern_table.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required field schema.",
                    ))
                })?;

                let constraints = (create_extern_table.constraints.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required table constraints.",
                    ))
                })?;
                let definition = if !create_extern_table.definition.is_empty() {
                    Some(create_extern_table.definition.clone())
                } else {
                    None
                };

                let file_type = create_extern_table.file_type.as_str();
                if ctx.table_factory(file_type).is_none() {
                    internal_err!("No TableProviderFactory for file type: {file_type}")?
                }

                let mut order_exprs = vec![];
                for expr in &create_extern_table.order_exprs {
                    let order_expr = expr
                        .logical_expr_nodes
                        .iter()
                        .map(|expr| from_proto::parse_expr(expr, ctx))
                        .collect::<Result<Vec<Expr>, _>>()?;
                    order_exprs.push(order_expr)
                }

                Ok(LogicalPlan::Ddl(DdlStatement::CreateExternalTable(CreateExternalTable {
                    schema: pb_schema.try_into()?,
                    name: from_owned_table_reference(create_extern_table.name.as_ref(), "CreateExternalTable")?,
                    location: create_extern_table.location.clone(),
                    file_type: create_extern_table.file_type.clone(),
                    has_header: create_extern_table.has_header,
                    delimiter: create_extern_table.delimiter.chars().next().ok_or_else(|| {
                        DataFusionError::Internal(String::from("Protobuf deserialization error, unable to parse CSV delimiter"))
                    })?,
                    table_partition_cols: create_extern_table
                        .table_partition_cols
                        .clone(),
                    order_exprs,
                    if_not_exists: create_extern_table.if_not_exists,
                    file_compression_type: CompressionTypeVariant::from_str(&create_extern_table.file_compression_type).map_err(|_| DataFusionError::NotImplemented(format!("Unsupported file compression type {}", create_extern_table.file_compression_type)))?,
                    definition,
                    unbounded: create_extern_table.unbounded,
                    options: create_extern_table.options.clone(),
                    constraints: constraints.into(),
                })))
            }
            LogicalPlanType::CreateView(create_view) => {
                let plan = create_view
                    .input.clone().ok_or_else(|| DataFusionError::Internal(String::from(
                    "Protobuf deserialization error, CreateViewNode has invalid LogicalPlan input.",
                )))?
                    .try_into_logical_plan(ctx, extension_codec)?;
                let definition = if !create_view.definition.is_empty() {
                    Some(create_view.definition.clone())
                } else {
                    None
                };

                Ok(LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                    name: from_owned_table_reference(
                        create_view.name.as_ref(),
                        "CreateView",
                    )?,
                    input: Arc::new(plan),
                    or_replace: create_view.or_replace,
                    definition,
                })))
            }
            LogicalPlanType::CreateCatalogSchema(create_catalog_schema) => {
                let pb_schema = (create_catalog_schema.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateCatalogSchemaNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(
                    CreateCatalogSchema {
                        schema_name: create_catalog_schema.schema_name.clone(),
                        if_not_exists: create_catalog_schema.if_not_exists,
                        schema: pb_schema.try_into()?,
                    },
                )))
            }
            LogicalPlanType::CreateCatalog(create_catalog) => {
                let pb_schema = (create_catalog.schema.clone()).ok_or_else(|| {
                    DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, CreateCatalogNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::Ddl(DdlStatement::CreateCatalog(
                    CreateCatalog {
                        catalog_name: create_catalog.catalog_name.clone(),
                        if_not_exists: create_catalog.if_not_exists,
                        schema: pb_schema.try_into()?,
                    },
                )))
            }
            LogicalPlanType::Analyze(analyze) => {
                let input: LogicalPlan =
                    into_logical_plan!(analyze.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(analyze.verbose, true)?
                    .build()
            }
            LogicalPlanType::Explain(explain) => {
                let input: LogicalPlan =
                    into_logical_plan!(explain.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(explain.verbose, false)?
                    .build()
            }
            LogicalPlanType::SubqueryAlias(aliased_relation) => {
                let input: LogicalPlan =
                    into_logical_plan!(aliased_relation.input, ctx, extension_codec)?;
                let alias = from_owned_table_reference(
                    aliased_relation.alias.as_ref(),
                    "SubqueryAlias",
                )?;
                LogicalPlanBuilder::from(input).alias(alias)?.build()
            }
            LogicalPlanType::Limit(limit) => {
                let input: LogicalPlan =
                    into_logical_plan!(limit.input, ctx, extension_codec)?;
                let skip = limit.skip.max(0) as usize;

                let fetch = if limit.fetch < 0 {
                    None
                } else {
                    Some(limit.fetch as usize)
                };

                LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
            }
            LogicalPlanType::Join(join) => {
                let left_keys: Vec<Expr> = join
                    .left_join_key
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let right_keys: Vec<Expr> = join
                    .right_join_key
                    .iter()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                let join_type =
                    protobuf::JoinType::try_from(join.join_type).map_err(|_| {
                        proto_error(format!(
                            "Received a JoinNode message with unknown JoinType {}",
                            join.join_type
                        ))
                    })?;
                let join_constraint = protobuf::JoinConstraint::try_from(
                    join.join_constraint,
                )
                .map_err(|_| {
                    proto_error(format!(
                        "Received a JoinNode message with unknown JoinConstraint {}",
                        join.join_constraint
                    ))
                })?;
                let filter: Option<Expr> = join
                    .filter
                    .as_ref()
                    .map(|expr| from_proto::parse_expr(expr, ctx))
                    .map_or(Ok(None), |v| v.map(Some))?;

                let builder = LogicalPlanBuilder::from(into_logical_plan!(
                    join.left,
                    ctx,
                    extension_codec
                )?);
                let builder = match join_constraint.into() {
                    JoinConstraint::On => builder.join_with_expr_keys(
                        into_logical_plan!(join.right, ctx, extension_codec)?,
                        join_type.into(),
                        (left_keys, right_keys),
                        filter,
                    )?,
                    JoinConstraint::Using => {
                        // The equijoin keys in using-join must be column.
                        let using_keys = left_keys
                            .into_iter()
                            .map(|key| key.try_into_col())
                            .collect::<Result<Vec<_>, _>>()?;
                        builder.join_using(
                            into_logical_plan!(join.right, ctx, extension_codec)?,
                            join_type.into(),
                            using_keys,
                        )?
                    }
                };

                builder.build()
            }
            LogicalPlanType::Union(union) => {
                let mut input_plans: Vec<LogicalPlan> = union
                    .inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_>>()?;

                if input_plans.len() < 2 {
                    return  Err( DataFusionError::Internal(String::from(
                        "Protobuf deserialization error, Union was require at least two input.",
                    )));
                }

                let first = input_plans.pop().ok_or_else(|| DataFusionError::Internal(String::from(
                    "Protobuf deserialization error, Union was require at least two input.",
                )))?;
                let mut builder = LogicalPlanBuilder::from(first);
                for plan in input_plans {
                    builder = builder.union(plan)?;
                }
                builder.build()
            }
            LogicalPlanType::CrossJoin(crossjoin) => {
                let left = into_logical_plan!(crossjoin.left, ctx, extension_codec)?;
                let right = into_logical_plan!(crossjoin.right, ctx, extension_codec)?;

                LogicalPlanBuilder::from(left).cross_join(right)?.build()
            }
            LogicalPlanType::Extension(LogicalExtensionNode { node, inputs }) => {
                let input_plans: Vec<LogicalPlan> = inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_>>()?;

                let extension_node =
                    extension_codec.try_decode(node, &input_plans, ctx)?;
                Ok(LogicalPlan::Extension(extension_node))
            }
            LogicalPlanType::Distinct(distinct) => {
                let input: LogicalPlan =
                    into_logical_plan!(distinct.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input).distinct()?.build()
            }
            LogicalPlanType::ViewScan(scan) => {
                let schema: Schema = convert_required!(scan.schema)?;

                let mut projection = None;
                if let Some(columns) = &scan.projection {
                    let column_indices = columns
                        .columns
                        .iter()
                        .map(|name| schema.index_of(name))
                        .collect::<Result<Vec<usize>, _>>()?;
                    projection = Some(column_indices);
                }

                let input: LogicalPlan =
                    into_logical_plan!(scan.input, ctx, extension_codec)?;

                let definition = if !scan.definition.is_empty() {
                    Some(scan.definition.clone())
                } else {
                    None
                };

                let provider = ViewTable::try_new(input, definition)?;

                let table_name =
                    from_owned_table_reference(scan.table_name.as_ref(), "ViewScan")?;

                LogicalPlanBuilder::scan(
                    table_name,
                    provider_as_source(Arc::new(provider)),
                    projection,
                )?
                .build()
            }
            LogicalPlanType::Prepare(prepare) => {
                let input: LogicalPlan =
                    into_logical_plan!(prepare.input, ctx, extension_codec)?;
                // FIXME Use LogicalType in proto
                let data_types: Vec<LogicalType> = prepare
                    .data_types
                    .iter()
                    .map(DataType::try_from)
                    .map(Into::into)
                    .collect::<Result<_, _>>()?;
                LogicalPlanBuilder::from(input)
                    .prepare(prepare.name.clone(), data_types)?
                    .build()
            }
            LogicalPlanType::DropView(dropview) => Ok(datafusion_expr::LogicalPlan::Ddl(
                datafusion_expr::DdlStatement::DropView(DropView {
                    name: from_owned_table_reference(dropview.name.as_ref(), "DropView")?,
                    if_exists: dropview.if_exists,
                    schema: Arc::new(convert_required!(dropview.schema)?),
                }),
            )),
        }
    }

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        match plan {
            LogicalPlan::Values(Values { values, .. }) => {
                let n_cols = if values.is_empty() {
                    0
                } else {
                    values[0].len()
                } as u64;
                let values_list = values
                    .iter()
                    .flatten()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Values(
                        protobuf::ValuesNode {
                            n_cols,
                            values_list,
                        },
                    )),
                })
            }
            LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                filters,
                projection,
                ..
            }) => {
                let provider = source_as_provider(source)?;
                let schema = provider.schema();
                let source = provider.as_any();

                let projection = match projection {
                    None => None,
                    Some(columns) => {
                        let column_names = columns
                            .iter()
                            .map(|i| schema.field(*i).name().to_owned())
                            .collect();
                        Some(protobuf::ProjectionColumns {
                            columns: column_names,
                        })
                    }
                };
                let schema: protobuf::Schema = schema.as_ref().try_into()?;

                let filters: Vec<protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                if let Some(listing_table) = source.downcast_ref::<ListingTable>() {
                    let any = listing_table.options().format.as_any();
                    let file_format_type = {
                        let mut maybe_some_type = None;

                        #[cfg(feature = "parquet")]
                        if any.is::<ParquetFormat>() {
                            maybe_some_type =
                                Some(FileFormatType::Parquet(protobuf::ParquetFormat {}))
                        };

                        if let Some(csv) = any.downcast_ref::<CsvFormat>() {
                            maybe_some_type =
                                Some(FileFormatType::Csv(protobuf::CsvFormat {
                                    delimiter: byte_to_string(
                                        csv.delimiter(),
                                        "delimiter",
                                    )?,
                                    has_header: csv.has_header(),
                                    quote: byte_to_string(csv.quote(), "quote")?,
                                    optional_escape: if let Some(escape) = csv.escape() {
                                        Some(
                                            protobuf::csv_format::OptionalEscape::Escape(
                                                byte_to_string(escape, "escape")?,
                                            ),
                                        )
                                    } else {
                                        None
                                    },
                                }))
                        }

                        if any.is::<AvroFormat>() {
                            maybe_some_type =
                                Some(FileFormatType::Avro(protobuf::AvroFormat {}))
                        }

                        if let Some(file_format_type) = maybe_some_type {
                            file_format_type
                        } else {
                            return Err(proto_error(format!(
                            "Error converting file format, {:?} is invalid as a datafusion format.",
                            listing_table.options().format
                        )));
                        }
                    };

                    let options = listing_table.options();

                    let mut exprs_vec: Vec<LogicalExprNodeCollection> = vec![];
                    for order in &options.file_sort_order {
                        let expr_vec = LogicalExprNodeCollection {
                            logical_expr_nodes: order
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, to_proto::Error>>()?,
                        };
                        exprs_vec.push(expr_vec);
                    }

                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ListingScan(
                            protobuf::ListingTableScanNode {
                                file_format_type: Some(file_format_type),
                                table_name: Some(table_name.clone().into()),
                                collect_stat: options.collect_stat,
                                file_extension: options.file_extension.clone(),
                                table_partition_cols: options
                                    .table_partition_cols
                                    .iter()
                                    .map(|x| x.0.clone())
                                    .collect::<Vec<_>>(),
                                paths: listing_table
                                    .table_paths()
                                    .iter()
                                    .map(|x| x.to_string())
                                    .collect(),
                                schema: Some(schema),
                                projection,
                                filters,
                                target_partitions: options.target_partitions as u32,
                                file_sort_order: exprs_vec,
                            },
                        )),
                    })
                } else if let Some(view_table) = source.downcast_ref::<ViewTable>() {
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ViewScan(Box::new(
                            protobuf::ViewTableScanNode {
                                table_name: Some(table_name.clone().into()),
                                input: Some(Box::new(
                                    protobuf::LogicalPlanNode::try_from_logical_plan(
                                        view_table.logical_plan(),
                                        extension_codec,
                                    )?,
                                )),
                                schema: Some(schema),
                                projection,
                                definition: view_table
                                    .definition()
                                    .map(|s| s.to_string())
                                    .unwrap_or_default(),
                            },
                        ))),
                    })
                } else {
                    let mut bytes = vec![];
                    extension_codec
                        .try_encode_table_provider(provider, &mut bytes)
                        .map_err(|e| context!("Error serializing custom table", e))?;
                    let scan = CustomScan(CustomTableScanNode {
                        table_name: Some(table_name.clone().into()),
                        projection,
                        schema: Some(schema),
                        filters,
                        custom_table_data: bytes,
                    });
                    let node = LogicalPlanNode {
                        logical_plan_type: Some(scan),
                    };
                    Ok(node)
                }
            }
            LogicalPlan::Projection(Projection { expr, input, .. }) => {
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Projection(Box::new(
                        protobuf::ProjectionNode {
                            input: Some(Box::new(
                                protobuf::LogicalPlanNode::try_from_logical_plan(
                                    input.as_ref(),
                                    extension_codec,
                                )?,
                            )),
                            expr: expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, to_proto::Error>>()?,
                            optional_alias: None,
                        },
                    ))),
                })
            }
            LogicalPlan::Filter(filter) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        filter.input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Selection(Box::new(
                        protobuf::SelectionNode {
                            input: Some(Box::new(input)),
                            expr: Some((&filter.predicate).try_into()?),
                        },
                    ))),
                })
            }
            LogicalPlan::Distinct(Distinct { input }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Distinct(Box::new(
                        protobuf::DistinctNode {
                            input: Some(Box::new(input)),
                        },
                    ))),
                })
            }
            LogicalPlan::Window(Window {
                input, window_expr, ..
            }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Window(Box::new(
                        protobuf::WindowNode {
                            input: Some(Box::new(input)),
                            window_expr: window_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Aggregate(Aggregate {
                group_expr,
                aggr_expr,
                input,
                ..
            }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Aggregate(Box::new(
                        protobuf::AggregateNode {
                            input: Some(Box::new(input)),
                            group_expr: group_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                            aggr_expr: aggr_expr
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                        },
                    ))),
                })
            }
            LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                null_equals_null,
                ..
            }) => {
                let left: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        left.as_ref(),
                        extension_codec,
                    )?;
                let right: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        right.as_ref(),
                        extension_codec,
                    )?;
                let (left_join_key, right_join_key) = on
                    .iter()
                    .map(|(l, r)| Ok((l.try_into()?, r.try_into()?)))
                    .collect::<Result<Vec<_>, to_proto::Error>>()?
                    .into_iter()
                    .unzip();
                let join_type: protobuf::JoinType = join_type.to_owned().into();
                let join_constraint: protobuf::JoinConstraint =
                    join_constraint.to_owned().into();
                let filter = filter
                    .as_ref()
                    .map(|e| e.try_into())
                    .map_or(Ok(None), |v| v.map(Some))?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Join(Box::new(
                        protobuf::JoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                            join_type: join_type.into(),
                            join_constraint: join_constraint.into(),
                            left_join_key,
                            right_join_key,
                            null_equals_null: *null_equals_null,
                            filter,
                        },
                    ))),
                })
            }
            LogicalPlan::Subquery(_) => {
                not_impl_err!("LogicalPlan serde is not yet implemented for subqueries")
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::SubqueryAlias(Box::new(
                        protobuf::SubqueryAliasNode {
                            input: Some(Box::new(input)),
                            alias: Some(alias.to_owned_reference().into()),
                        },
                    ))),
                })
            }
            LogicalPlan::Limit(Limit { input, skip, fetch }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Limit(Box::new(
                        protobuf::LimitNode {
                            input: Some(Box::new(input)),
                            skip: *skip as i64,
                            fetch: fetch.unwrap_or(i64::MAX as usize) as i64,
                        },
                    ))),
                })
            }
            LogicalPlan::Sort(Sort { input, expr, fetch }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                let selection_expr: Vec<protobuf::LogicalExprNode> = expr
                    .iter()
                    .map(|expr| expr.try_into())
                    .collect::<Result<Vec<_>, to_proto::Error>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Sort(Box::new(
                        protobuf::SortNode {
                            input: Some(Box::new(input)),
                            expr: selection_expr,
                            fetch: fetch.map(|f| f as i64).unwrap_or(-1i64),
                        },
                    ))),
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                use datafusion::logical_expr::Partitioning;
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;

                // Assumed common usize field was batch size
                // Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method = match partitioning_scheme {
                    Partitioning::Hash(exprs, partition_count) => {
                        PartitionMethod::Hash(protobuf::HashRepartition {
                            hash_expr: exprs
                                .iter()
                                .map(|expr| expr.try_into())
                                .collect::<Result<Vec<_>, to_proto::Error>>()?,
                            partition_count: *partition_count as u64,
                        })
                    }
                    Partitioning::RoundRobinBatch(partition_count) => {
                        PartitionMethod::RoundRobin(*partition_count as u64)
                    }
                    Partitioning::DistributeBy(_) => {
                        return not_impl_err!("DistributeBy")
                    }
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Repartition(Box::new(
                        protobuf::RepartitionNode {
                            input: Some(Box::new(input)),
                            partition_method: Some(pb_partition_method),
                        },
                    ))),
                })
            }
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row, ..
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::EmptyRelation(
                    protobuf::EmptyRelationNode {
                        produce_one_row: *produce_one_row,
                    },
                )),
            }),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(
                CreateExternalTable {
                    name,
                    location,
                    file_type,
                    has_header,
                    delimiter,
                    schema: df_schema,
                    table_partition_cols,
                    if_not_exists,
                    definition,
                    file_compression_type,
                    order_exprs,
                    unbounded,
                    options,
                    constraints,
                },
            )) => {
                let mut converted_order_exprs: Vec<LogicalExprNodeCollection> = vec![];
                for order in order_exprs {
                    let temp = LogicalExprNodeCollection {
                        logical_expr_nodes: order
                            .iter()
                            .map(|expr| expr.try_into())
                            .collect::<Result<Vec<_>, to_proto::Error>>(
                        )?,
                    };
                    converted_order_exprs.push(temp);
                }

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CreateExternalTable(
                        protobuf::CreateExternalTableNode {
                            name: Some(name.clone().into()),
                            location: location.clone(),
                            file_type: file_type.clone(),
                            has_header: *has_header,
                            schema: Some(df_schema.try_into()?),
                            table_partition_cols: table_partition_cols.clone(),
                            if_not_exists: *if_not_exists,
                            delimiter: String::from(*delimiter),
                            order_exprs: converted_order_exprs,
                            definition: definition.clone().unwrap_or_default(),
                            file_compression_type: file_compression_type.to_string(),
                            unbounded: *unbounded,
                            options: options.clone(),
                            constraints: Some(constraints.clone().into()),
                        },
                    )),
                })
            }
            LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
                name,
                input,
                or_replace,
                definition,
            })) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateView(Box::new(
                    protobuf::CreateViewNode {
                        name: Some(name.clone().into()),
                        input: Some(Box::new(LogicalPlanNode::try_from_logical_plan(
                            input,
                            extension_codec,
                        )?)),
                        or_replace: *or_replace,
                        definition: definition.clone().unwrap_or_default(),
                    },
                ))),
            }),
            LogicalPlan::Ddl(DdlStatement::CreateCatalogSchema(
                CreateCatalogSchema {
                    schema_name,
                    if_not_exists,
                    schema: df_schema,
                },
            )) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalogSchema(
                    protobuf::CreateCatalogSchemaNode {
                        schema_name: schema_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.try_into()?),
                    },
                )),
            }),
            LogicalPlan::Ddl(DdlStatement::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                schema: df_schema,
            })) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalog(
                    protobuf::CreateCatalogNode {
                        catalog_name: catalog_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.try_into()?),
                    },
                )),
            }),
            LogicalPlan::Analyze(a) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    a.input.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Analyze(Box::new(
                        protobuf::AnalyzeNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Explain(a) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    a.plan.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Explain(Box::new(
                        protobuf::ExplainNode {
                            input: Some(Box::new(input)),
                            verbose: a.verbose,
                        },
                    ))),
                })
            }
            LogicalPlan::Union(union) => {
                let inputs: Vec<LogicalPlanNode> = union
                    .inputs
                    .iter()
                    .map(|i| {
                        protobuf::LogicalPlanNode::try_from_logical_plan(
                            i,
                            extension_codec,
                        )
                    })
                    .collect::<Result<_>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Union(
                        protobuf::UnionNode { inputs },
                    )),
                })
            }
            LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
                let left = protobuf::LogicalPlanNode::try_from_logical_plan(
                    left.as_ref(),
                    extension_codec,
                )?;
                let right = protobuf::LogicalPlanNode::try_from_logical_plan(
                    right.as_ref(),
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CrossJoin(Box::new(
                        protobuf::CrossJoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                        },
                    ))),
                })
            }
            LogicalPlan::Extension(extension) => {
                let mut buf: Vec<u8> = vec![];
                extension_codec.try_encode(extension, &mut buf)?;

                let inputs: Vec<LogicalPlanNode> = extension
                    .node
                    .inputs()
                    .iter()
                    .map(|i| {
                        protobuf::LogicalPlanNode::try_from_logical_plan(
                            i,
                            extension_codec,
                        )
                    })
                    .collect::<Result<_>>()?;

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Extension(
                        LogicalExtensionNode { node: buf, inputs },
                    )),
                })
            }
            LogicalPlan::Prepare(Prepare {
                name,
                data_types,
                input,
            }) => {
                let input = protobuf::LogicalPlanNode::try_from_logical_plan(
                    input,
                    extension_codec,
                )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Prepare(Box::new(
                        protobuf::PrepareNode {
                            name: name.clone(),
                            data_types: data_types
                                .iter()
                                .map(|t| t.try_into())
                                .collect::<Result<Vec<_>, _>>()?,
                            input: Some(Box::new(input)),
                        },
                    ))),
                })
            }
            LogicalPlan::Unnest(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for Unnest",
            )),
            LogicalPlan::Ddl(DdlStatement::CreateMemoryTable(_)) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for CreateMemoryTable",
            )),
            LogicalPlan::Ddl(DdlStatement::DropTable(_)) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DropTable",
            )),
            LogicalPlan::Ddl(DdlStatement::DropView(DropView {
                name,
                if_exists,
                schema,
            })) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::DropView(
                    protobuf::DropViewNode {
                        name: Some(name.clone().into()),
                        if_exists: *if_exists,
                        schema: Some(schema.try_into()?),
                    },
                )),
            }),
            LogicalPlan::Ddl(DdlStatement::DropCatalogSchema(_)) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DropCatalogSchema",
            )),
            LogicalPlan::Statement(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for Statement",
            )),
            LogicalPlan::Dml(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for Dml",
            )),
            LogicalPlan::Copy(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for Copy",
            )),
            LogicalPlan::DescribeTable(_) => Err(proto_error(
                "LogicalPlan serde is not yet implemented for DescribeTable",
            )),
        }
    }
}
