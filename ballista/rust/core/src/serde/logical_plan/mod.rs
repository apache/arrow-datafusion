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

use crate::error::BallistaError;
use crate::serde::protobuf::LogicalExtensionNode;
use crate::serde::{
    byte_to_string, proto_error, protobuf, str_to_byte, AsLogicalPlan,
    LogicalExtensionCodec,
};
use crate::{convert_required, into_logical_plan};
use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::avro::AvroFormat;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::logical_plan::plan::{
    Aggregate, EmptyRelation, Filter, Join, Projection, Sort, SubqueryAlias, Window,
};
use datafusion::logical_plan::{
    source_as_provider, Column, CreateCatalog, CreateCatalogSchema, CreateExternalTable,
    CrossJoin, Expr, JoinConstraint, Limit, LogicalPlan, LogicalPlanBuilder, Repartition,
    TableScan, Values,
};
use datafusion::prelude::SessionContext;

use datafusion_proto::from_proto::parse_expr;
use prost::bytes::BufMut;
use prost::Message;
use protobuf::listing_table_scan_node::FileFormatType;
use protobuf::logical_plan_node::LogicalPlanType;
use protobuf::LogicalPlanNode;
use std::convert::TryInto;
use std::sync::Arc;

pub mod from_proto;

impl AsLogicalPlan for LogicalPlanNode {
    fn try_decode(buf: &[u8]) -> Result<Self, BallistaError>
    where
        Self: Sized,
    {
        LogicalPlanNode::decode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to decode logical plan: {:?}", e))
        })
    }

    fn try_encode<B>(&self, buf: &mut B) -> Result<(), BallistaError>
    where
        B: BufMut,
        Self: Sized,
    {
        self.encode(buf).map_err(|e| {
            BallistaError::Internal(format!("failed to encode logical plan: {:?}", e))
        })
    }

    fn try_into_logical_plan(
        &self,
        ctx: &SessionContext,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<LogicalPlan, BallistaError> {
        let plan = self.logical_plan_type.as_ref().ok_or_else(|| {
            proto_error(format!(
                "logical_plan::from_proto() Unsupported logical plan '{:?}'",
                self
            ))
        })?;
        match plan {
            LogicalPlanType::Values(values) => {
                let n_cols = values.n_cols as usize;
                let values: Vec<Vec<Expr>> = if values.values_list.is_empty() {
                    Ok(Vec::new())
                } else if values.values_list.len() % n_cols != 0 {
                    Err(BallistaError::General(format!(
                        "Invalid values list length, expect {} to be divisible by {}",
                        values.values_list.len(),
                        n_cols
                    )))
                } else {
                    values
                        .values_list
                        .chunks_exact(n_cols)
                        .map(|r| {
                            r.iter().map(|expr| parse_expr(expr, ctx)).collect::<Result<
                                Vec<_>,
                                datafusion_proto::from_proto::Error,
                            >>(
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| e.into())
                }?;
                LogicalPlanBuilder::values(values)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Projection(projection) => {
                let input: LogicalPlan =
                    into_logical_plan!(projection.input, ctx, extension_codec)?;
                let x: Vec<Expr> = projection
                    .expr
                    .iter()
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .project_with_alias(
                        x,
                        projection.optional_alias.as_ref().map(|a| match a {
                            protobuf::projection_node::OptionalAlias::Alias(alias) => {
                                alias.clone()
                            }
                        }),
                    )?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Selection(selection) => {
                let input: LogicalPlan =
                    into_logical_plan!(selection.input, ctx, extension_codec)?;
                let expr: Expr = selection
                    .expr
                    .as_ref()
                    .map(|expr| parse_expr(expr, ctx))
                    .transpose()?
                    .ok_or_else(|| {
                        BallistaError::General("expression required".to_string())
                    })?;
                // .try_into()?;
                LogicalPlanBuilder::from(input)
                    .filter(expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Window(window) => {
                let input: LogicalPlan =
                    into_logical_plan!(window.input, ctx, extension_codec)?;
                let window_expr = window
                    .window_expr
                    .iter()
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .window(window_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Aggregate(aggregate) => {
                let input: LogicalPlan =
                    into_logical_plan!(aggregate.input, ctx, extension_codec)?;
                let group_expr = aggregate
                    .group_expr
                    .iter()
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                let aggr_expr = aggregate
                    .aggr_expr
                    .iter()
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .aggregate(group_expr, aggr_expr)?
                    .build()
                    .map_err(|e| e.into())
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
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<_>, _>>()?;

                let file_format: Arc<dyn FileFormat> =
                    match scan.file_format_type.as_ref().ok_or_else(|| {
                        proto_error(format!(
                            "logical_plan::from_proto() Unsupported file format '{:?}'",
                            self
                        ))
                    })? {
                        &FileFormatType::Parquet(protobuf::ParquetFormat {
                            enable_pruning,
                        }) => Arc::new(
                            ParquetFormat::default().with_enable_pruning(enable_pruning),
                        ),
                        FileFormatType::Csv(protobuf::CsvFormat {
                            has_header,
                            delimiter,
                        }) => Arc::new(
                            CsvFormat::default()
                                .with_has_header(*has_header)
                                .with_delimiter(str_to_byte(delimiter)?),
                        ),
                        FileFormatType::Avro(..) => Arc::new(AvroFormat::default()),
                    };

                let options = ListingOptions {
                    file_extension: scan.file_extension.clone(),
                    format: file_format,
                    table_partition_cols: scan.table_partition_cols.clone(),
                    collect_stat: scan.collect_stat,
                    target_partitions: scan.target_partitions as usize,
                };

                let object_store = ctx
                    .runtime_env()
                    .object_store(scan.path.as_str())
                    .map_err(|e| {
                        BallistaError::NotImplemented(format!(
                            "No object store is registered for path {}: {:?}",
                            scan.path, e
                        ))
                    })?
                    .0;

                println!(
                    "Found object store {:?} for path {}",
                    object_store,
                    scan.path.as_str()
                );

                let config = ListingTableConfig::new(object_store, scan.path.as_str())
                    .with_listing_options(options)
                    .with_schema(Arc::new(schema));

                let provider = ListingTable::try_new(config)?;

                LogicalPlanBuilder::scan_with_filters(
                    &scan.table_name,
                    Arc::new(provider),
                    projection,
                    filters,
                )?
                .build()
                .map_err(|e| e.into())
            }
            LogicalPlanType::Sort(sort) => {
                let input: LogicalPlan =
                    into_logical_plan!(sort.input, ctx, extension_codec)?;
                let sort_expr: Vec<Expr> = sort
                    .expr
                    .iter()
                    .map(|expr| parse_expr(expr, ctx))
                    .collect::<Result<Vec<Expr>, _>>()?;
                LogicalPlanBuilder::from(input)
                    .sort(sort_expr)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Repartition(repartition) => {
                use datafusion::logical_plan::Partitioning;
                let input: LogicalPlan =
                    into_logical_plan!(repartition.input, ctx, extension_codec)?;
                use protobuf::repartition_node::PartitionMethod;
                let pb_partition_method = repartition.partition_method.clone().ok_or_else(|| {
                    BallistaError::General(String::from(
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
                            .map(|expr| parse_expr(expr, ctx))
                            .collect::<Result<Vec<_>, _>>()?,
                        partition_count as usize,
                    ),
                    PartitionMethod::RoundRobin(partition_count) => {
                        Partitioning::RoundRobinBatch(partition_count as usize)
                    }
                };

                LogicalPlanBuilder::from(input)
                    .repartition(partitioning_scheme)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::EmptyRelation(empty_relation) => {
                LogicalPlanBuilder::empty(empty_relation.produce_one_row)
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::CreateExternalTable(create_extern_table) => {
                let pb_schema = (create_extern_table.schema.clone()).ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, CreateExternalTableNode was missing required field schema.",
                    ))
                })?;

                let pb_file_type: protobuf::FileType =
                    create_extern_table.file_type.try_into()?;

                Ok(LogicalPlan::CreateExternalTable(CreateExternalTable {
                    schema: pb_schema.try_into()?,
                    name: create_extern_table.name.clone(),
                    location: create_extern_table.location.clone(),
                    file_type: pb_file_type.into(),
                    has_header: create_extern_table.has_header,
                    delimiter: create_extern_table.delimiter.chars().next().ok_or_else(|| {
                        BallistaError::General(String::from("Protobuf deserialization error, unable to parse CSV delimiter"))
                    })?,
                    table_partition_cols: create_extern_table
                        .table_partition_cols
                        .clone(),
                    if_not_exists: create_extern_table.if_not_exists,
                }))
            }
            LogicalPlanType::CreateCatalogSchema(create_catalog_schema) => {
                let pb_schema = (create_catalog_schema.schema.clone()).ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, CreateCatalogSchemaNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                    schema_name: create_catalog_schema.schema_name.clone(),
                    if_not_exists: create_catalog_schema.if_not_exists,
                    schema: pb_schema.try_into()?,
                }))
            }
            LogicalPlanType::CreateCatalog(create_catalog) => {
                let pb_schema = (create_catalog.schema.clone()).ok_or_else(|| {
                    BallistaError::General(String::from(
                        "Protobuf deserialization error, CreateCatalogNode was missing required field schema.",
                    ))
                })?;

                Ok(LogicalPlan::CreateCatalog(CreateCatalog {
                    catalog_name: create_catalog.catalog_name.clone(),
                    if_not_exists: create_catalog.if_not_exists,
                    schema: pb_schema.try_into()?,
                }))
            }
            LogicalPlanType::Analyze(analyze) => {
                let input: LogicalPlan =
                    into_logical_plan!(analyze.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(analyze.verbose, true)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Explain(explain) => {
                let input: LogicalPlan =
                    into_logical_plan!(explain.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .explain(explain.verbose, false)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::SubqueryAlias(aliased_relation) => {
                let input: LogicalPlan =
                    into_logical_plan!(aliased_relation.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .alias(&aliased_relation.alias)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Limit(limit) => {
                let input: LogicalPlan =
                    into_logical_plan!(limit.input, ctx, extension_codec)?;
                LogicalPlanBuilder::from(input)
                    .limit(limit.limit as usize)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Join(join) => {
                let left_keys: Vec<Column> =
                    join.left_join_column.iter().map(|i| i.into()).collect();
                let right_keys: Vec<Column> =
                    join.right_join_column.iter().map(|i| i.into()).collect();
                let join_type =
                    protobuf::JoinType::from_i32(join.join_type).ok_or_else(|| {
                        proto_error(format!(
                            "Received a JoinNode message with unknown JoinType {}",
                            join.join_type
                        ))
                    })?;
                let join_constraint = protobuf::JoinConstraint::from_i32(
                    join.join_constraint,
                )
                .ok_or_else(|| {
                    proto_error(format!(
                        "Received a JoinNode message with unknown JoinConstraint {}",
                        join.join_constraint
                    ))
                })?;

                let builder = LogicalPlanBuilder::from(into_logical_plan!(
                    join.left,
                    ctx,
                    extension_codec
                )?);
                let builder = match join_constraint.into() {
                    JoinConstraint::On => builder.join(
                        &into_logical_plan!(join.right, ctx, extension_codec)?,
                        join_type.into(),
                        (left_keys, right_keys),
                    )?,
                    JoinConstraint::Using => builder.join_using(
                        &into_logical_plan!(join.right, ctx, extension_codec)?,
                        join_type.into(),
                        left_keys,
                    )?,
                };

                builder.build().map_err(|e| e.into())
            }
            LogicalPlanType::Union(union) => {
                let mut input_plans: Vec<LogicalPlan> = union
                    .inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_, BallistaError>>()?;

                if input_plans.len() < 2 {
                    return  Err( BallistaError::General(String::from(
                       "Protobuf deserialization error, Union was require at least two input.",
                   )));
                }

                let mut builder = LogicalPlanBuilder::from(input_plans.pop().unwrap());
                for plan in input_plans {
                    builder = builder.union(plan)?;
                }
                builder.build().map_err(|e| e.into())
            }
            LogicalPlanType::CrossJoin(crossjoin) => {
                let left = into_logical_plan!(crossjoin.left, ctx, extension_codec)?;
                let right = into_logical_plan!(crossjoin.right, ctx, extension_codec)?;

                LogicalPlanBuilder::from(left)
                    .cross_join(&right)?
                    .build()
                    .map_err(|e| e.into())
            }
            LogicalPlanType::Extension(LogicalExtensionNode { node, inputs }) => {
                let input_plans: Vec<LogicalPlan> = inputs
                    .iter()
                    .map(|i| i.try_into_logical_plan(ctx, extension_codec))
                    .collect::<Result<_, BallistaError>>()?;

                let extension_node =
                    extension_codec.try_decode(node, &input_plans, ctx)?;
                Ok(LogicalPlan::Extension(extension_node))
            }
        }
    }

    fn try_from_logical_plan(
        plan: &LogicalPlan,
        extension_codec: &dyn LogicalExtensionCodec,
    ) -> Result<Self, BallistaError>
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
                let source = source_as_provider(source)?;
                let schema = source.schema();
                let source = source.as_any();

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
                let schema: datafusion_proto::protobuf::Schema = schema.as_ref().into();

                let filters: Vec<datafusion_proto::protobuf::LogicalExprNode> = filters
                    .iter()
                    .map(|filter| filter.try_into())
                    .collect::<Result<Vec<_>, _>>()?;

                if let Some(listing_table) = source.downcast_ref::<ListingTable>() {
                    let any = listing_table.options().format.as_any();
                    let file_format_type = if let Some(parquet) =
                        any.downcast_ref::<ParquetFormat>()
                    {
                        FileFormatType::Parquet(protobuf::ParquetFormat {
                            enable_pruning: parquet.enable_pruning(),
                        })
                    } else if let Some(csv) = any.downcast_ref::<CsvFormat>() {
                        FileFormatType::Csv(protobuf::CsvFormat {
                            delimiter: byte_to_string(csv.delimiter())?,
                            has_header: csv.has_header(),
                        })
                    } else if any.is::<AvroFormat>() {
                        FileFormatType::Avro(protobuf::AvroFormat {})
                    } else {
                        return Err(proto_error(format!(
                            "Error converting file format, {:?} is invalid as a datafusion foramt.",
                            listing_table.options().format
                        )));
                    };
                    Ok(protobuf::LogicalPlanNode {
                        logical_plan_type: Some(LogicalPlanType::ListingScan(
                            protobuf::ListingTableScanNode {
                                file_format_type: Some(file_format_type),
                                table_name: table_name.to_owned(),
                                collect_stat: listing_table.options().collect_stat,
                                file_extension: listing_table
                                    .options()
                                    .file_extension
                                    .clone(),
                                table_partition_cols: listing_table
                                    .options()
                                    .table_partition_cols
                                    .clone(),
                                path: listing_table.table_path().to_owned(),
                                schema: Some(schema),
                                projection,
                                filters,
                                target_partitions: listing_table
                                    .options()
                                    .target_partitions
                                    as u32,
                            },
                        )),
                    })
                } else {
                    Err(BallistaError::General(format!(
                        "logical plan to_proto unsupported table provider {:?}",
                        source
                    )))
                }
            }
            LogicalPlan::Projection(Projection {
                expr, input, alias, ..
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::Projection(Box::new(
                    protobuf::ProjectionNode {
                        input: Some(Box::new(
                            protobuf::LogicalPlanNode::try_from_logical_plan(
                                input.as_ref(),
                                extension_codec,
                            )?,
                        )),
                        expr: expr.iter().map(|expr| expr.try_into()).collect::<Result<
                            Vec<_>,
                            datafusion_proto::to_proto::Error,
                        >>(
                        )?,
                        optional_alias: alias
                            .clone()
                            .map(protobuf::projection_node::OptionalAlias::Alias),
                    },
                ))),
            }),
            LogicalPlan::Filter(Filter { predicate, input }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Selection(Box::new(
                        protobuf::SelectionNode {
                            input: Some(Box::new(input)),
                            expr: Some(predicate.try_into()?),
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
                let (left_join_column, right_join_column) =
                    on.iter().map(|(l, r)| (l.into(), r.into())).unzip();
                let join_type: protobuf::JoinType = join_type.to_owned().into();
                let join_constraint: protobuf::JoinConstraint =
                    join_constraint.to_owned().into();
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Join(Box::new(
                        protobuf::JoinNode {
                            left: Some(Box::new(left)),
                            right: Some(Box::new(right)),
                            join_type: join_type.into(),
                            join_constraint: join_constraint.into(),
                            left_join_column,
                            right_join_column,
                            null_equals_null: *null_equals_null,
                        },
                    ))),
                })
            }
            LogicalPlan::Subquery(_) => {
                // note that the ballista and datafusion proto files need refactoring to allow
                // LogicalExprNode to reference a LogicalPlanNode
                // see https://github.com/apache/arrow-datafusion/issues/2338
                Err(BallistaError::NotImplemented(
                    "Ballista does not support subqueries".to_string(),
                ))
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
                            alias: alias.clone(),
                        },
                    ))),
                })
            }
            LogicalPlan::Limit(Limit { input, n }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Limit(Box::new(
                        protobuf::LimitNode {
                            input: Some(Box::new(input)),
                            limit: *n as u32,
                        },
                    ))),
                })
            }
            LogicalPlan::Sort(Sort { input, expr }) => {
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;
                let selection_expr: Vec<datafusion_proto::protobuf::LogicalExprNode> =
                    expr.iter()
                        .map(|expr| expr.try_into())
                        .collect::<Result<Vec<_>, datafusion_proto::to_proto::Error>>()?;
                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Sort(Box::new(
                        protobuf::SortNode {
                            input: Some(Box::new(input)),
                            expr: selection_expr,
                        },
                    ))),
                })
            }
            LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }) => {
                use datafusion::logical_plan::Partitioning;
                let input: protobuf::LogicalPlanNode =
                    protobuf::LogicalPlanNode::try_from_logical_plan(
                        input.as_ref(),
                        extension_codec,
                    )?;

                // Assumed common usize field was batch size
                // Used u64 to avoid any nastyness involving large values, most data clusters are probably uniformly 64 bits any ways
                use protobuf::repartition_node::PartitionMethod;

                let pb_partition_method =
                    match partitioning_scheme {
                        Partitioning::Hash(exprs, partition_count) => {
                            PartitionMethod::Hash(protobuf::HashRepartition {
                                hash_expr: exprs
                                    .iter()
                                    .map(|expr| expr.try_into())
                                    .collect::<Result<
                                    Vec<_>,
                                    datafusion_proto::to_proto::Error,
                                >>()?,
                                partition_count: *partition_count as u64,
                            })
                        }
                        Partitioning::RoundRobinBatch(partition_count) => {
                            PartitionMethod::RoundRobin(*partition_count as u64)
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
            LogicalPlan::CreateExternalTable(CreateExternalTable {
                name,
                location,
                file_type,
                has_header,
                delimiter,
                schema: df_schema,
                table_partition_cols,
                if_not_exists,
            }) => {
                use datafusion::logical_plan::FileType;

                let pb_file_type: protobuf::FileType = match file_type {
                    FileType::NdJson => protobuf::FileType::NdJson,
                    FileType::Parquet => protobuf::FileType::Parquet,
                    FileType::CSV => protobuf::FileType::Csv,
                    FileType::Avro => protobuf::FileType::Avro,
                };

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::CreateExternalTable(
                        protobuf::CreateExternalTableNode {
                            name: name.clone(),
                            location: location.clone(),
                            file_type: pb_file_type as i32,
                            has_header: *has_header,
                            schema: Some(df_schema.into()),
                            table_partition_cols: table_partition_cols.clone(),
                            if_not_exists: *if_not_exists,
                            delimiter: String::from(*delimiter),
                        },
                    )),
                })
            }
            LogicalPlan::CreateCatalogSchema(CreateCatalogSchema {
                schema_name,
                if_not_exists,
                schema: df_schema,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalogSchema(
                    protobuf::CreateCatalogSchemaNode {
                        schema_name: schema_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.into()),
                    },
                )),
            }),
            LogicalPlan::CreateCatalog(CreateCatalog {
                catalog_name,
                if_not_exists,
                schema: df_schema,
            }) => Ok(protobuf::LogicalPlanNode {
                logical_plan_type: Some(LogicalPlanType::CreateCatalog(
                    protobuf::CreateCatalogNode {
                        catalog_name: catalog_name.clone(),
                        if_not_exists: *if_not_exists,
                        schema: Some(df_schema.into()),
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
                    .collect::<Result<_, BallistaError>>()?;
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
                    .collect::<Result<_, BallistaError>>()?;

                Ok(protobuf::LogicalPlanNode {
                    logical_plan_type: Some(LogicalPlanType::Extension(
                        LogicalExtensionNode { node: buf, inputs },
                    )),
                })
            }
            LogicalPlan::CreateMemoryTable(_) => Err(proto_error(
                "Error converting CreateMemoryTable. Not yet supported in Ballista",
            )),
            LogicalPlan::DropTable(_) => Err(proto_error(
                "Error converting DropTable. Not yet supported in Ballista",
            )),
            LogicalPlan::TableUDFs(_) => Err(proto_error(
                "Error converting TableUDFs. Not yet supported in Ballista",
            )),
        }
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

#[cfg(test)]
mod roundtrip_tests {

    use super::super::{super::error::Result, protobuf};
    use crate::error::BallistaError;
    use crate::serde::{AsLogicalPlan, BallistaCodec};
    use async_trait::async_trait;
    use core::panic;
    use datafusion::logical_plan::source_as_provider;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datafusion_data_access::{
            self,
            object_store::{
                local::LocalFileSystem, FileMetaStream, ListEntryStream, ObjectReader,
                ObjectStore,
            },
            SizedFile,
        },
        datasource::listing::ListingTable,
        logical_plan::{
            col, CreateExternalTable, Expr, FileType, LogicalPlan, LogicalPlanBuilder,
            Repartition, ToDFSchema,
        },
        prelude::*,
    };
    use std::io;
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestObjectStore {}

    #[async_trait]
    impl ObjectStore for TestObjectStore {
        async fn list_file(
            &self,
            _prefix: &str,
        ) -> datafusion_data_access::Result<FileMetaStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }

        async fn list_dir(
            &self,
            _prefix: &str,
            _delimiter: Option<String>,
        ) -> datafusion_data_access::Result<ListEntryStream> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }

        fn file_reader(
            &self,
            _file: SizedFile,
        ) -> datafusion_data_access::Result<Arc<dyn ObjectReader>> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "this is only a test object store".to_string(),
            ))
        }
    }

    // Given a identity of a LogicalPlan converts it to protobuf and back, using debug formatting to test equality.
    macro_rules! roundtrip_test {
        ($initial_struct:ident, $proto_type:ty, $struct_type:ty) => {
            let proto: $proto_type = (&$initial_struct).try_into()?;

            let round_trip: $struct_type = (&proto).try_into()?;

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
        ($initial_struct:ident, $struct_type:ty) => {
            roundtrip_test!($initial_struct, protobuf::LogicalPlanNode, $struct_type);
        };
        ($initial_struct:ident) => {
            let ctx = SessionContext::new();
            let codec: BallistaCodec<
                protobuf::LogicalPlanNode,
                protobuf::PhysicalPlanNode,
            > = BallistaCodec::default();
            let proto: protobuf::LogicalPlanNode =
                protobuf::LogicalPlanNode::try_from_logical_plan(
                    &$initial_struct,
                    codec.logical_extension_codec(),
                )
                .expect("from logical plan");
            let round_trip: LogicalPlan = proto
                .try_into_logical_plan(&ctx, codec.logical_extension_codec())
                .expect("to logical plan");

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
        ($initial_struct:ident, $ctx:ident) => {
            let codec: BallistaCodec<
                protobuf::LogicalPlanNode,
                protobuf::PhysicalPlanNode,
            > = BallistaCodec::default();
            let proto: protobuf::LogicalPlanNode =
                protobuf::LogicalPlanNode::try_from_logical_plan(&$initial_struct)
                    .expect("from logical plan");
            let round_trip: LogicalPlan = proto
                .try_into_logical_plan(&$ctx, codec.logical_extension_codec())
                .expect("to logical plan");

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
    }

    #[tokio::test]
    async fn roundtrip_repartition() -> Result<()> {
        use datafusion::logical_plan::Partitioning;

        let test_partition_counts = [usize::MIN, usize::MAX, 43256];

        let test_expr: Vec<Expr> =
            vec![col("c1") + col("c2"), Expr::Literal((4.0).into())];

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = std::sync::Arc::new(
            LogicalPlanBuilder::scan_csv(
                Arc::new(LocalFileSystem {}),
                "employee.csv",
                CsvReadOptions::new().schema(&schema).has_header(true),
                Some(vec![3, 4]),
                4,
            )
            .await
            .and_then(|plan| plan.sort(vec![col("salary")]))
            .and_then(|plan| plan.build())
            .map_err(BallistaError::DataFusionError)?,
        );

        for partition_count in test_partition_counts.iter() {
            let rr_repartition = Partitioning::RoundRobinBatch(*partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: rr_repartition,
            });

            roundtrip_test!(roundtrip_plan);

            let h_repartition = Partitioning::Hash(test_expr.clone(), *partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: h_repartition,
            });

            roundtrip_test!(roundtrip_plan);

            let no_expr_hrepartition = Partitioning::Hash(Vec::new(), *partition_count);

            let roundtrip_plan = LogicalPlan::Repartition(Repartition {
                input: plan.clone(),
                partitioning_scheme: no_expr_hrepartition,
            });

            roundtrip_test!(roundtrip_plan);
        }

        Ok(())
    }

    #[test]
    fn roundtrip_create_external_table() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let df_schema_ref = schema.to_dfschema_ref()?;

        let filetypes: [FileType; 4] = [
            FileType::NdJson,
            FileType::Parquet,
            FileType::CSV,
            FileType::Avro,
        ];

        for file in filetypes.iter() {
            let create_table_node =
                LogicalPlan::CreateExternalTable(CreateExternalTable {
                    schema: df_schema_ref.clone(),
                    name: String::from("TestName"),
                    location: String::from("employee.csv"),
                    file_type: *file,
                    has_header: true,
                    delimiter: ',',
                    table_partition_cols: vec![],
                    if_not_exists: false,
                });

            roundtrip_test!(create_table_node);
        }

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_analyze() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let verbose_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(true, true))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        let plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(false, true))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);

        roundtrip_test!(verbose_plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_explain() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let verbose_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(true, false))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        let plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.explain(false, false))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);

        roundtrip_test!(verbose_plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_join() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let scan_plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee1",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![0, 3, 4]),
            4,
        )
        .await?
        .build()
        .map_err(BallistaError::DataFusionError)?;

        let plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee2",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![0, 3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.join(&scan_plan, JoinType::Inner, (vec!["id"], vec!["id"])))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);
        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_sort() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.sort(vec![col("salary")]))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;
        roundtrip_test!(plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_empty_relation() -> Result<()> {
        let plan_false = LogicalPlanBuilder::empty(false)
            .build()
            .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan_false);

        let plan_true = LogicalPlanBuilder::empty(true)
            .build()
            .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan_true);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            "employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.aggregate(vec![col("state")], vec![max(col("salary"))]))
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        roundtrip_test!(plan);

        Ok(())
    }

    #[tokio::test]
    async fn roundtrip_logical_plan_custom_ctx() -> Result<()> {
        let ctx = SessionContext::new();
        let codec: BallistaCodec<protobuf::LogicalPlanNode, protobuf::PhysicalPlanNode> =
            BallistaCodec::default();
        let custom_object_store = Arc::new(TestObjectStore {});
        ctx.runtime_env()
            .register_object_store("test", custom_object_store.clone());

        let (os, _) = ctx.runtime_env().object_store("test://foo.csv")?;

        println!("Object Store {:?}", os);

        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ]);

        let plan = LogicalPlanBuilder::scan_csv(
            custom_object_store.clone(),
            "test://employee.csv",
            CsvReadOptions::new().schema(&schema).has_header(true),
            Some(vec![3, 4]),
            4,
        )
        .await
        .and_then(|plan| plan.build())
        .map_err(BallistaError::DataFusionError)?;

        let proto: protobuf::LogicalPlanNode =
            protobuf::LogicalPlanNode::try_from_logical_plan(
                &plan,
                codec.logical_extension_codec(),
            )
            .expect("from logical plan");
        let round_trip: LogicalPlan = proto
            .try_into_logical_plan(&ctx, codec.logical_extension_codec())
            .expect("to logical plan");

        assert_eq!(format!("{:?}", plan), format!("{:?}", round_trip));

        let round_trip_store = match round_trip {
            LogicalPlan::TableScan(scan) => {
                let source = source_as_provider(&scan.source)?;
                match source.as_ref().as_any().downcast_ref::<ListingTable>() {
                    Some(listing_table) => {
                        format!("{:?}", listing_table.object_store())
                    }
                    _ => panic!("expected a ListingTable"),
                }
            }
            _ => panic!("expected a TableScan"),
        };

        assert_eq!(round_trip_store, format!("{:?}", custom_object_store));

        Ok(())
    }
}
