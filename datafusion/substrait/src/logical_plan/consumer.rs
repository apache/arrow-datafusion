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

use async_recursion::async_recursion;
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{
    not_impl_err, substrait_datafusion_err, substrait_err, DFSchema, DFSchemaRef,
};

use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    aggregate_function, expr::find_df_window_func, BinaryExpr, BuiltinScalarFunction,
    Case, Expr, LogicalPlan, Operator,
};
use datafusion::logical_expr::{
    expr, Cast, Extension, GroupingSet, Like, LogicalPlanBuilder, Partitioning,
    Repartition, ScalarUDF, Subquery, WindowFrameBound, WindowFrameUnits,
};
use datafusion::prelude::JoinType;
use datafusion::sql::TableReference;
use datafusion::{
    error::{DataFusionError, Result},
    logical_expr::utils::split_conjunction,
    prelude::{Column, SessionContext},
    scalar::ScalarValue,
};
use substrait::proto::exchange_rel::ExchangeKind;
use substrait::proto::expression::subquery::SubqueryType;
use substrait::proto::expression::{FieldReference, Literal, ScalarFunction};
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind, window_function::Bound,
        MaskExpression, RexType,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    set_rel,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, Expression, Plan, Rel, Type,
};
use substrait::proto::{FunctionArgument, SortField};

use datafusion::common::plan_err;
use datafusion::logical_expr::expr::{InList, InSubquery, Sort};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::variation_const::{
    DATE_32_TYPE_REF, DATE_64_TYPE_REF, DECIMAL_128_TYPE_REF, DECIMAL_256_TYPE_REF,
    DEFAULT_CONTAINER_TYPE_REF, DEFAULT_TYPE_REF, LARGE_CONTAINER_TYPE_REF,
    TIMESTAMP_MICRO_TYPE_REF, TIMESTAMP_MILLI_TYPE_REF, TIMESTAMP_NANO_TYPE_REF,
    TIMESTAMP_SECOND_TYPE_REF, UNSIGNED_INTEGER_TYPE_REF,
};

enum ScalarFunctionType {
    Builtin(BuiltinScalarFunction),
    Op(Operator),
    Expr(BuiltinExprBuilder),
    Udf(Arc<ScalarUDF>),
}

pub fn name_to_op(name: &str) -> Result<Operator> {
    match name {
        "equal" => Ok(Operator::Eq),
        "not_equal" => Ok(Operator::NotEq),
        "lt" => Ok(Operator::Lt),
        "lte" => Ok(Operator::LtEq),
        "gt" => Ok(Operator::Gt),
        "gte" => Ok(Operator::GtEq),
        "add" => Ok(Operator::Plus),
        "subtract" => Ok(Operator::Minus),
        "multiply" => Ok(Operator::Multiply),
        "divide" => Ok(Operator::Divide),
        "mod" => Ok(Operator::Modulo),
        "and" => Ok(Operator::And),
        "or" => Ok(Operator::Or),
        "is_distinct_from" => Ok(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Ok(Operator::IsNotDistinctFrom),
        "regex_match" => Ok(Operator::RegexMatch),
        "regex_imatch" => Ok(Operator::RegexIMatch),
        "regex_not_match" => Ok(Operator::RegexNotMatch),
        "regex_not_imatch" => Ok(Operator::RegexNotIMatch),
        "bitwise_and" => Ok(Operator::BitwiseAnd),
        "bitwise_or" => Ok(Operator::BitwiseOr),
        "str_concat" => Ok(Operator::StringConcat),
        "at_arrow" => Ok(Operator::AtArrow),
        "arrow_at" => Ok(Operator::ArrowAt),
        "bitwise_xor" => Ok(Operator::BitwiseXor),
        "bitwise_shift_right" => Ok(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Ok(Operator::BitwiseShiftLeft),
        _ => not_impl_err!("Unsupported function name: {name:?}"),
    }
}

fn scalar_function_type_from_str(
    ctx: &SessionContext,
    name: &str,
) -> Result<ScalarFunctionType> {
    let s = ctx.state();
    if let Some(func) = s.scalar_functions().get(name) {
        return Ok(ScalarFunctionType::Udf(func.to_owned()));
    }

    if let Ok(op) = name_to_op(name) {
        return Ok(ScalarFunctionType::Op(op));
    }

    if let Ok(fun) = BuiltinScalarFunction::from_str(name) {
        return Ok(ScalarFunctionType::Builtin(fun));
    }

    if let Some(builder) = BuiltinExprBuilder::try_from_name(name) {
        return Ok(ScalarFunctionType::Expr(builder));
    }

    not_impl_err!("Unsupported function name: {name:?}")
}

fn split_eq_and_noneq_join_predicate_with_nulls_equality(
    filter: &Expr,
) -> (Vec<(Column, Column)>, bool, Option<Expr>) {
    let exprs = split_conjunction(filter);

    let mut accum_join_keys: Vec<(Column, Column)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    let mut nulls_equal_nulls = false;

    for expr in exprs {
        match expr {
            Expr::BinaryExpr(binary_expr) => match binary_expr {
                x @ (BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }
                | BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                }) => {
                    nulls_equal_nulls = match x.op {
                        Operator::Eq => false,
                        Operator::IsNotDistinctFrom => true,
                        _ => unreachable!(),
                    };

                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(l), Expr::Column(r)) => {
                            accum_join_keys.push((l.clone(), r.clone()));
                        }
                        _ => accum_filters.push(expr.clone()),
                    }
                }
                _ => accum_filters.push(expr.clone()),
            },
            _ => accum_filters.push(expr.clone()),
        }
    }

    let join_filter = accum_filters.into_iter().reduce(Expr::and);
    (accum_join_keys, nulls_equal_nulls, join_filter)
}

/// Convert Substrait Plan to DataFusion LogicalPlan
pub async fn from_substrait_plan(
    ctx: &SessionContext,
    plan: &Plan,
) -> Result<LogicalPlan> {
    // Register function extension
    let function_extension = plan
        .extensions
        .iter()
        .map(|e| match &e.mapping_type {
            Some(ext) => match ext {
                MappingType::ExtensionFunction(ext_f) => {
                    Ok((ext_f.function_anchor, &ext_f.name))
                }
                _ => not_impl_err!("Extension type not supported: {ext:?}"),
            },
            None => not_impl_err!("Cannot parse empty extension"),
        })
        .collect::<Result<HashMap<_, _>>>()?;
    // Parse relations
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => {
                        Ok(from_substrait_rel(ctx, rel, &function_extension).await?)
                    },
                    plan_rel::RelType::Root(root) => {
                        Ok(from_substrait_rel(ctx, root.input.as_ref().unwrap(), &function_extension).await?)
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    ctx: &SessionContext,
    rel: &Rel,
    extensions: &HashMap<u32, &String>,
) -> Result<LogicalPlan> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            if let Some(input) = p.input.as_ref() {
                let mut input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut exprs: Vec<Expr> = vec![];
                for e in &p.expressions {
                    let x =
                        from_substrait_rex(ctx, e, input.clone().schema(), extensions)
                            .await?;
                    // if the expression is WindowFunction, wrap in a Window relation
                    //   before returning and do not add to list of this Projection's expression list
                    // otherwise, add expression to the Projection's expression list
                    match &*x {
                        Expr::WindowFunction(_) => {
                            input = input.window(vec![x.as_ref().clone()])?;
                            exprs.push(x.as_ref().clone());
                        }
                        _ => {
                            exprs.push(x.as_ref().clone());
                        }
                    }
                }
                input.project(exprs)?.build()
            } else {
                not_impl_err!("Projection without an input is not supported")
            }
        }
        Some(RelType::Filter(filter)) => {
            if let Some(input) = filter.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                if let Some(condition) = filter.condition.as_ref() {
                    let expr =
                        from_substrait_rex(ctx, condition, input.schema(), extensions)
                            .await?;
                    input.filter(expr.as_ref().clone())?.build()
                } else {
                    not_impl_err!("Filter without an condition is not valid")
                }
            } else {
                not_impl_err!("Filter without an input is not valid")
            }
        }
        Some(RelType::Fetch(fetch)) => {
            if let Some(input) = fetch.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let offset = fetch.offset as usize;
                // Since protobuf can't directly distinguish `None` vs `0` `None` is encoded as `MAX`
                let count = if fetch.count as usize == usize::MAX {
                    None
                } else {
                    Some(fetch.count as usize)
                };
                input.limit(offset, count)?.build()
            } else {
                not_impl_err!("Fetch without an input is not valid")
            }
        }
        Some(RelType::Sort(sort)) => {
            if let Some(input) = sort.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let sorts =
                    from_substrait_sorts(ctx, &sort.sorts, input.schema(), extensions)
                        .await?;
                input.sort(sorts)?.build()
            } else {
                not_impl_err!("Sort without an input is not valid")
            }
        }
        Some(RelType::Aggregate(agg)) => {
            if let Some(input) = agg.input.as_ref() {
                let input = LogicalPlanBuilder::from(
                    from_substrait_rel(ctx, input, extensions).await?,
                );
                let mut group_expr = vec![];
                let mut aggr_expr = vec![];

                match agg.groupings.len() {
                    1 => {
                        for e in &agg.groupings[0].grouping_expressions {
                            let x =
                                from_substrait_rex(ctx, e, input.schema(), extensions)
                                    .await?;
                            group_expr.push(x.as_ref().clone());
                        }
                    }
                    _ => {
                        let mut grouping_sets = vec![];
                        for grouping in &agg.groupings {
                            let mut grouping_set = vec![];
                            for e in &grouping.grouping_expressions {
                                let x = from_substrait_rex(
                                    ctx,
                                    e,
                                    input.schema(),
                                    extensions,
                                )
                                .await?;
                                grouping_set.push(x.as_ref().clone());
                            }
                            grouping_sets.push(grouping_set);
                        }
                        // Single-element grouping expression of type Expr::GroupingSet.
                        // Note that GroupingSet::Rollup would become GroupingSet::GroupingSets, when
                        // parsed by the producer and consumer, since Substrait does not have a type dedicated
                        // to ROLLUP. Only vector of Groupings (grouping sets) is available.
                        group_expr.push(Expr::GroupingSet(GroupingSet::GroupingSets(
                            grouping_sets,
                        )));
                    }
                };

                for m in &agg.measures {
                    let filter = match &m.filter {
                        Some(fil) => Some(Box::new(
                            from_substrait_rex(ctx, fil, input.schema(), extensions)
                                .await?
                                .as_ref()
                                .clone(),
                        )),
                        None => None,
                    };
                    let agg_func = match &m.measure {
                        Some(f) => {
                            let distinct = match f.invocation {
                                _ if f.invocation
                                    == AggregationInvocation::Distinct as i32 =>
                                {
                                    true
                                }
                                _ if f.invocation
                                    == AggregationInvocation::All as i32 =>
                                {
                                    false
                                }
                                _ => false,
                            };
                            from_substrait_agg_func(
                                ctx,
                                f,
                                input.schema(),
                                extensions,
                                filter,
                                // TODO: Add parsing of order_by also
                                None,
                                distinct,
                            )
                            .await
                        }
                        None => not_impl_err!(
                            "Aggregate without aggregate function is not supported"
                        ),
                    };
                    aggr_expr.push(agg_func?.as_ref().clone());
                }

                input.aggregate(group_expr, aggr_expr)?.build()
            } else {
                not_impl_err!("Aggregate without an input is not valid")
            }
        }
        Some(RelType::Join(join)) => {
            if join.post_join_filter.is_some() {
                return not_impl_err!(
                    "JoinRel with post_join_filter is not yet supported"
                );
            }

            let left: LogicalPlanBuilder = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.left.as_ref().unwrap(), extensions).await?,
            );
            let right = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, join.right.as_ref().unwrap(), extensions).await?,
            );
            let join_type = from_substrait_jointype(join.r#type)?;
            // The join condition expression needs full input schema and not the output schema from join since we lose columns from
            // certain join types such as semi and anti joins
            let in_join_schema = left.schema().join(right.schema())?;

            // If join expression exists, parse the `on` condition expression, build join and return
            // Otherwise, build join with only the filter, without join keys
            match &join.expression.as_ref() {
                Some(expr) => {
                    let on = from_substrait_rex(ctx, expr, &in_join_schema, extensions)
                        .await?;
                    // The join expression can contain both equal and non-equal ops.
                    // As of datafusion 31.0.0, the equal and non equal join conditions are in separate fields.
                    // So we extract each part as follows:
                    // - If an Eq or IsNotDistinctFrom op is encountered, add the left column, right column and is_null_equal_nulls to `join_ons` vector
                    // - Otherwise we add the expression to join_filter (use conjunction if filter already exists)
                    let (join_ons, nulls_equal_nulls, join_filter) =
                        split_eq_and_noneq_join_predicate_with_nulls_equality(&on);
                    let (left_cols, right_cols): (Vec<_>, Vec<_>) =
                        itertools::multiunzip(join_ons);
                    left.join_detailed(
                        right.build()?,
                        join_type,
                        (left_cols, right_cols),
                        join_filter,
                        nulls_equal_nulls,
                    )?
                    .build()
                }
                None => plan_err!("JoinRel without join condition is not allowed"),
            }
        }
        Some(RelType::Cross(cross)) => {
            let left: LogicalPlanBuilder = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, cross.left.as_ref().unwrap(), extensions).await?,
            );
            let right =
                from_substrait_rel(ctx, cross.right.as_ref().unwrap(), extensions)
                    .await?;
            left.cross_join(right)?.build()
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_reference = match nt.names.len() {
                    0 => {
                        return plan_err!("No table name found in NamedTable");
                    }
                    1 => TableReference::Bare {
                        table: (&nt.names[0]).into(),
                    },
                    2 => TableReference::Partial {
                        schema: (&nt.names[0]).into(),
                        table: (&nt.names[1]).into(),
                    },
                    _ => TableReference::Full {
                        catalog: (&nt.names[0]).into(),
                        schema: (&nt.names[1]).into(),
                        table: (&nt.names[2]).into(),
                    },
                };
                let t = ctx.table(table_reference).await?;
                let t = t.into_optimized_plan()?;
                match &read.projection {
                    Some(MaskExpression { select, .. }) => match &select.as_ref() {
                        Some(projection) => {
                            let column_indices: Vec<usize> = projection
                                .struct_items
                                .iter()
                                .map(|item| item.field as usize)
                                .collect();
                            match &t {
                                LogicalPlan::TableScan(scan) => {
                                    let fields = column_indices
                                        .iter()
                                        .map(|i| {
                                            scan.projected_schema.qualified_field(*i)
                                        })
                                        .map(|(qualifier, field)| {
                                            (qualifier.cloned(), Arc::new(field.clone()))
                                        })
                                        .collect();
                                    let mut scan = scan.clone();
                                    scan.projection = Some(column_indices);
                                    scan.projected_schema =
                                        DFSchemaRef::new(DFSchema::new_with_metadata(
                                            fields,
                                            HashMap::new(),
                                        )?);
                                    Ok(LogicalPlan::TableScan(scan))
                                }
                                _ => plan_err!("unexpected plan for table"),
                            }
                        }
                        _ => Ok(t),
                    },
                    _ => Ok(t),
                }
            }
            _ => not_impl_err!("Only NamedTable reads are supported"),
        },
        Some(RelType::Set(set)) => match set_rel::SetOp::try_from(set.op) {
            Ok(set_op) => match set_op {
                set_rel::SetOp::UnionAll => {
                    if !set.inputs.is_empty() {
                        let mut union_builder = Ok(LogicalPlanBuilder::from(
                            from_substrait_rel(ctx, &set.inputs[0], extensions).await?,
                        ));
                        for input in &set.inputs[1..] {
                            union_builder = union_builder?
                                .union(from_substrait_rel(ctx, input, extensions).await?);
                        }
                        union_builder?.build()
                    } else {
                        not_impl_err!("Union relation requires at least one input")
                    }
                }
                _ => not_impl_err!("Unsupported set operator: {set_op:?}"),
            },
            Err(e) => not_impl_err!("Invalid set operation type {}: {e}", set.op),
        },
        Some(RelType::ExtensionLeaf(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionLeafRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionSingle(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            let Some(input_rel) = &extension.input else {
                return substrait_err!(
                    "ExtensionSingleRel doesn't contains input rel. Try use ExtensionLeafRel instead"
                );
            };
            let input_plan = from_substrait_rel(ctx, input_rel, extensions).await?;
            let plan = plan.from_template(&plan.expressions(), &[input_plan]);
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::ExtensionMulti(extension)) => {
            let Some(ext_detail) = &extension.detail else {
                return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
            };
            let plan = ctx
                .state()
                .serializer_registry()
                .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
            let mut inputs = Vec::with_capacity(extension.inputs.len());
            for input in &extension.inputs {
                let input_plan = from_substrait_rel(ctx, input, extensions).await?;
                inputs.push(input_plan);
            }
            let plan = plan.from_template(&plan.expressions(), &inputs);
            Ok(LogicalPlan::Extension(Extension { node: plan }))
        }
        Some(RelType::Exchange(exchange)) => {
            let Some(input) = exchange.input.as_ref() else {
                return substrait_err!("Unexpected empty input in ExchangeRel");
            };
            let input = Arc::new(from_substrait_rel(ctx, input, extensions).await?);

            let Some(exchange_kind) = &exchange.exchange_kind else {
                return substrait_err!("Unexpected empty input in ExchangeRel");
            };

            // ref: https://substrait.io/relations/physical_relations/#exchange-types
            let partitioning_scheme = match exchange_kind {
                ExchangeKind::ScatterByFields(scatter_fields) => {
                    let mut partition_columns = vec![];
                    let input_schema = input.schema();
                    for field_ref in &scatter_fields.fields {
                        let column =
                            from_substrait_field_reference(field_ref, input_schema)?;
                        partition_columns.push(column);
                    }
                    Partitioning::Hash(
                        partition_columns,
                        exchange.partition_count as usize,
                    )
                }
                ExchangeKind::RoundRobin(_) => {
                    Partitioning::RoundRobinBatch(exchange.partition_count as usize)
                }
                ExchangeKind::SingleTarget(_)
                | ExchangeKind::MultiTarget(_)
                | ExchangeKind::Broadcast(_) => {
                    return not_impl_err!("Unsupported exchange kind: {exchange_kind:?}");
                }
            };
            Ok(LogicalPlan::Repartition(Repartition {
                input,
                partitioning_scheme,
            }))
        }
        _ => not_impl_err!("Unsupported RelType: {:?}", rel.rel_type),
    }
}

fn from_substrait_jointype(join_type: i32) -> Result<JoinType> {
    if let Ok(substrait_join_type) = join_rel::JoinType::try_from(join_type) {
        match substrait_join_type {
            join_rel::JoinType::Inner => Ok(JoinType::Inner),
            join_rel::JoinType::Left => Ok(JoinType::Left),
            join_rel::JoinType::Right => Ok(JoinType::Right),
            join_rel::JoinType::Outer => Ok(JoinType::Full),
            join_rel::JoinType::Anti => Ok(JoinType::LeftAnti),
            join_rel::JoinType::Semi => Ok(JoinType::LeftSemi),
            _ => plan_err!("unsupported join type {substrait_join_type:?}"),
        }
    } else {
        plan_err!("invalid join type variant {join_type:?}")
    }
}

/// Convert Substrait Sorts to DataFusion Exprs
pub async fn from_substrait_sorts(
    ctx: &SessionContext,
    substrait_sorts: &Vec<SortField>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut sorts: Vec<Expr> = vec![];
    for s in substrait_sorts {
        let expr =
            from_substrait_rex(ctx, s.expr.as_ref().unwrap(), input_schema, extensions)
                .await?;
        let asc_nullfirst = match &s.sort_kind {
            Some(k) => match k {
                Direction(d) => {
                    let Ok(direction) = SortDirection::try_from(*d) else {
                        return not_impl_err!(
                            "Unsupported Substrait SortDirection value {d}"
                        );
                    };

                    match direction {
                        SortDirection::AscNullsFirst => Ok((true, true)),
                        SortDirection::AscNullsLast => Ok((true, false)),
                        SortDirection::DescNullsFirst => Ok((false, true)),
                        SortDirection::DescNullsLast => Ok((false, false)),
                        SortDirection::Clustered => not_impl_err!(
                            "Sort with direction clustered is not yet supported"
                        ),
                        SortDirection::Unspecified => {
                            not_impl_err!("Unspecified sort direction is invalid")
                        }
                    }
                }
                ComparisonFunctionReference(_) => not_impl_err!(
                    "Sort using comparison function reference is not supported"
                ),
            },
            None => not_impl_err!("Sort without sort kind is invalid"),
        };
        let (asc, nulls_first) = asc_nullfirst.unwrap();
        sorts.push(Expr::Sort(Sort {
            expr: Box::new(expr.as_ref().clone()),
            asc,
            nulls_first,
        }));
    }
    Ok(sorts)
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    ctx: &SessionContext,
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = from_substrait_rex(ctx, expr, input_schema, extensions).await?;
        expressions.push(expression.as_ref().clone());
    }
    Ok(expressions)
}

/// Convert Substrait FunctionArguments to DataFusion Exprs
pub async fn from_substriat_func_args(
    ctx: &SessionContext,
    arguments: &Vec<FunctionArgument>,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Vec<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => {
                from_substrait_rex(ctx, e, input_schema, extensions).await
            }
            _ => {
                not_impl_err!("Aggregated function argument non-Value type not supported")
            }
        };
        args.push(arg_expr?.as_ref().clone());
    }
    Ok(args)
}

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    ctx: &SessionContext,
    f: &AggregateFunction,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
    filter: Option<Box<Expr>>,
    order_by: Option<Vec<Expr>>,
    distinct: bool,
) -> Result<Arc<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in &f.arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => {
                from_substrait_rex(ctx, e, input_schema, extensions).await
            }
            _ => {
                not_impl_err!("Aggregated function argument non-Value type not supported")
            }
        };
        args.push(arg_expr?.as_ref().clone());
    }

    let Some(function_name) = extensions.get(&f.function_reference) else {
        return plan_err!(
            "Aggregate function not registered: function anchor = {:?}",
            f.function_reference
        );
    };

    // try udaf first, then built-in aggr fn.
    if let Ok(fun) = ctx.udaf(function_name) {
        Ok(Arc::new(Expr::AggregateFunction(
            expr::AggregateFunction::new_udf(fun, args, distinct, filter, order_by),
        )))
    } else if let Ok(fun) = aggregate_function::AggregateFunction::from_str(function_name)
    {
        Ok(Arc::new(Expr::AggregateFunction(
            expr::AggregateFunction::new(fun, args, distinct, filter, order_by, None),
        )))
    } else {
        not_impl_err!(
            "Aggregated function {} is not supported: function anchor = {:?}",
            function_name,
            f.function_reference
        )
    }
}

/// Convert Substrait Rex to DataFusion Expr
#[async_recursion]
pub async fn from_substrait_rex(
    ctx: &SessionContext,
    e: &Expression,
    input_schema: &DFSchema,
    extensions: &HashMap<u32, &String>,
) -> Result<Arc<Expr>> {
    match &e.rex_type {
        Some(RexType::SingularOrList(s)) => {
            let substrait_expr = s.value.as_ref().unwrap();
            let substrait_list = s.options.as_ref();
            Ok(Arc::new(Expr::InList(InList {
                expr: Box::new(
                    from_substrait_rex(ctx, substrait_expr, input_schema, extensions)
                        .await?
                        .as_ref()
                        .clone(),
                ),
                list: from_substrait_rex_vec(
                    ctx,
                    substrait_list,
                    input_schema,
                    extensions,
                )
                .await?,
                negated: false,
            })))
        }
        Some(RexType::Selection(field_ref)) => Ok(Arc::new(
            from_substrait_field_reference(field_ref, input_schema)?,
        )),
        Some(RexType::IfThen(if_then)) => {
            // Parse `ifs`
            // If the first element does not have a `then` part, then we can assume it's a base expression
            let mut when_then_expr: Vec<(Box<Expr>, Box<Expr>)> = vec![];
            let mut expr = None;
            for (i, if_expr) in if_then.ifs.iter().enumerate() {
                if i == 0 {
                    // Check if the first element is type base expression
                    if if_expr.then.is_none() {
                        expr = Some(Box::new(
                            from_substrait_rex(
                                ctx,
                                if_expr.r#if.as_ref().unwrap(),
                                input_schema,
                                extensions,
                            )
                            .await?
                            .as_ref()
                            .clone(),
                        ));
                        continue;
                    }
                }
                when_then_expr.push((
                    Box::new(
                        from_substrait_rex(
                            ctx,
                            if_expr.r#if.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?
                        .as_ref()
                        .clone(),
                    ),
                    Box::new(
                        from_substrait_rex(
                            ctx,
                            if_expr.then.as_ref().unwrap(),
                            input_schema,
                            extensions,
                        )
                        .await?
                        .as_ref()
                        .clone(),
                    ),
                ));
            }
            // Parse `else`
            let else_expr = match &if_then.r#else {
                Some(e) => Some(Box::new(
                    from_substrait_rex(ctx, e, input_schema, extensions)
                        .await?
                        .as_ref()
                        .clone(),
                )),
                None => None,
            };
            Ok(Arc::new(Expr::Case(Case {
                expr,
                when_then_expr,
                else_expr,
            })))
        }
        Some(RexType::ScalarFunction(f)) => {
            let fn_name = extensions.get(&f.function_reference).ok_or_else(|| {
                DataFusionError::NotImplemented(format!(
                    "Aggregated function not found: function reference = {:?}",
                    f.function_reference
                ))
            })?;

            // Convert function arguments from Substrait to DataFusion
            async fn decode_arguments(
                ctx: &SessionContext,
                input_schema: &DFSchema,
                extensions: &HashMap<u32, &String>,
                function_args: &[FunctionArgument],
            ) -> Result<Vec<Expr>> {
                let mut args = Vec::with_capacity(function_args.len());
                for arg in function_args {
                    let arg_expr = match &arg.arg_type {
                        Some(ArgType::Value(e)) => {
                            from_substrait_rex(ctx, e, input_schema, extensions).await
                        }
                        _ => not_impl_err!(
                            "Aggregated function argument non-Value type not supported"
                        ),
                    }?;
                    args.push(arg_expr.as_ref().clone());
                }
                Ok(args)
            }

            let fn_type = scalar_function_type_from_str(ctx, fn_name)?;
            match fn_type {
                ScalarFunctionType::Udf(fun) => {
                    let args = decode_arguments(
                        ctx,
                        input_schema,
                        extensions,
                        f.arguments.as_slice(),
                    )
                    .await?;
                    Ok(Arc::new(Expr::ScalarFunction(
                        expr::ScalarFunction::new_udf(fun, args),
                    )))
                }
                ScalarFunctionType::Builtin(fun) => {
                    let args = decode_arguments(
                        ctx,
                        input_schema,
                        extensions,
                        f.arguments.as_slice(),
                    )
                    .await?;
                    Ok(Arc::new(Expr::ScalarFunction(expr::ScalarFunction::new(
                        fun, args,
                    ))))
                }
                ScalarFunctionType::Op(op) => {
                    if f.arguments.len() != 2 {
                        return not_impl_err!(
                            "Expect two arguments for binary operator {op:?}"
                        );
                    }
                    let lhs = &f.arguments[0].arg_type;
                    let rhs = &f.arguments[1].arg_type;

                    match (lhs, rhs) {
                        (Some(ArgType::Value(l)), Some(ArgType::Value(r))) => {
                            Ok(Arc::new(Expr::BinaryExpr(BinaryExpr {
                                left: Box::new(
                                    from_substrait_rex(ctx, l, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                                op,
                                right: Box::new(
                                    from_substrait_rex(ctx, r, input_schema, extensions)
                                        .await?
                                        .as_ref()
                                        .clone(),
                                ),
                            })))
                        }
                        (l, r) => not_impl_err!(
                            "Invalid arguments for binary expression: {l:?} and {r:?}"
                        ),
                    }
                }
                ScalarFunctionType::Expr(builder) => {
                    builder.build(ctx, f, input_schema, extensions).await
                }
            }
        }
        Some(RexType::Literal(lit)) => {
            let scalar_value = from_substrait_literal(lit)?;
            Ok(Arc::new(Expr::Literal(scalar_value)))
        }
        Some(RexType::Cast(cast)) => match cast.as_ref().r#type.as_ref() {
            Some(output_type) => Ok(Arc::new(Expr::Cast(Cast::new(
                Box::new(
                    from_substrait_rex(
                        ctx,
                        cast.as_ref().input.as_ref().unwrap().as_ref(),
                        input_schema,
                        extensions,
                    )
                    .await?
                    .as_ref()
                    .clone(),
                ),
                from_substrait_type(output_type)?,
            )))),
            None => substrait_err!("Cast experssion without output type is not allowed"),
        },
        Some(RexType::WindowFunction(window)) => {
            let fun = match extensions.get(&window.function_reference) {
                Some(function_name) => Ok(find_df_window_func(function_name)),
                None => not_impl_err!(
                    "Window function not found: function anchor = {:?}",
                    &window.function_reference
                ),
            };
            let order_by =
                from_substrait_sorts(ctx, &window.sorts, input_schema, extensions)
                    .await?;
            // Substrait does not encode WindowFrameUnits so we're using a simple logic to determine the units
            // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
            // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
            // TODO: Consider the cases where window frame is specified in query and is different from default
            let units = if order_by.is_empty() {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Range
            };
            Ok(Arc::new(Expr::WindowFunction(expr::WindowFunction {
                fun: fun?.unwrap(),
                args: from_substriat_func_args(
                    ctx,
                    &window.arguments,
                    input_schema,
                    extensions,
                )
                .await?,
                partition_by: from_substrait_rex_vec(
                    ctx,
                    &window.partitions,
                    input_schema,
                    extensions,
                )
                .await?,
                order_by,
                window_frame: datafusion::logical_expr::WindowFrame::new_bounds(
                    units,
                    from_substrait_bound(&window.lower_bound, true)?,
                    from_substrait_bound(&window.upper_bound, false)?,
                ),
                null_treatment: None,
            })))
        }
        Some(RexType::Subquery(subquery)) => match &subquery.as_ref().subquery_type {
            Some(subquery_type) => match subquery_type {
                SubqueryType::InPredicate(in_predicate) => {
                    if in_predicate.needles.len() != 1 {
                        Err(DataFusionError::Substrait(
                            "InPredicate Subquery type must have exactly one Needle expression"
                                .to_string(),
                        ))
                    } else {
                        let needle_expr = &in_predicate.needles[0];
                        let haystack_expr = &in_predicate.haystack;
                        if let Some(haystack_expr) = haystack_expr {
                            let haystack_expr =
                                from_substrait_rel(ctx, haystack_expr, extensions)
                                    .await?;
                            let outer_refs = haystack_expr.all_out_ref_exprs();
                            Ok(Arc::new(Expr::InSubquery(InSubquery {
                                expr: Box::new(
                                    from_substrait_rex(
                                        ctx,
                                        needle_expr,
                                        input_schema,
                                        extensions,
                                    )
                                    .await?
                                    .as_ref()
                                    .clone(),
                                ),
                                subquery: Subquery {
                                    subquery: Arc::new(haystack_expr),
                                    outer_ref_columns: outer_refs,
                                },
                                negated: false,
                            })))
                        } else {
                            substrait_err!("InPredicate Subquery type must have a Haystack expression")
                        }
                    }
                }
                _ => substrait_err!("Subquery type not implemented"),
            },
            None => {
                substrait_err!("Subquery experssion without SubqueryType is not allowed")
            }
        },
        _ => not_impl_err!("unsupported rex_type"),
    }
}

fn from_substrait_type(dt: &substrait::proto::Type) -> Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt8),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt16),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt32),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_REF => Ok(DataType::UInt64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Second, None))
                }
                TIMESTAMP_MILLI_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                }
                TIMESTAMP_MICRO_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                }
                TIMESTAMP_NANO_TYPE_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(DataType::Date32),
                DATE_64_TYPE_REF => Ok(DataType::Date64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeBinary),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length))
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeUtf8),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::List(list) => {
                let inner_type =
                    from_substrait_type(list.r#type.as_ref().ok_or_else(|| {
                        substrait_datafusion_err!("List type must have inner type")
                    })?)?;
                let field = Arc::new(Field::new("list_item", inner_type, true));
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_REF => Ok(DataType::List(field)),
                    LARGE_CONTAINER_TYPE_REF => Ok(DataType::LargeList(field)),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8))
                }
                DECIMAL_256_TYPE_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            _ => not_impl_err!("Unsupported Substrait type: {s_kind:?}"),
        },
        _ => not_impl_err!("`None` Substrait kind is not supported"),
    }
}

fn from_substrait_bound(
    bound: &Option<Bound>,
    is_lower: bool,
) -> Result<WindowFrameBound> {
    match bound {
        Some(b) => match &b.kind {
            Some(k) => match k {
                BoundKind::CurrentRow(SubstraitBound::CurrentRow {}) => {
                    Ok(WindowFrameBound::CurrentRow)
                }
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => Ok(
                    WindowFrameBound::Preceding(ScalarValue::Int64(Some(*offset))),
                ),
                BoundKind::Following(SubstraitBound::Following { offset }) => Ok(
                    WindowFrameBound::Following(ScalarValue::Int64(Some(*offset))),
                ),
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => substrait_err!("WindowFunction missing Substrait Bound kind"),
        },
        None => {
            if is_lower {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            } else {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
        }
    }
}

pub(crate) fn from_substrait_literal(lit: &Literal) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_REF => ScalarValue::TimestampSecond(Some(*t), None),
            TIMESTAMP_MILLI_TYPE_REF => ScalarValue::TimestampMillisecond(Some(*t), None),
            TIMESTAMP_MICRO_TYPE_REF => ScalarValue::TimestampMicrosecond(Some(*t), None),
            TIMESTAMP_NANO_TYPE_REF => ScalarValue::TimestampNanosecond(Some(*t), None),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_REF => ScalarValue::LargeBinary(Some(b.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d
                .value
                .clone()
                .try_into()
                .or(substrait_err!("Failed to parse decimal value"))?;
            let p = d.precision.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal precision: {e}")
            })?;
            let s = d.scale.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal scale: {e}")
            })?;
            ScalarValue::Decimal128(
                Some(std::primitive::i128::from_le_bytes(value)),
                p,
                s,
            )
        }
        Some(LiteralType::Null(ntype)) => from_substrait_null(ntype)?,
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

fn from_substrait_null(null_type: &Type) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::Bool(_) => Ok(ScalarValue::Boolean(None)),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int8(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int16(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt16(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int32(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt32(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_REF => Ok(ScalarValue::Int64(None)),
                UNSIGNED_INTEGER_TYPE_REF => Ok(ScalarValue::UInt64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(ScalarValue::Float32(None)),
            r#type::Kind::Fp64(_) => Ok(ScalarValue::Float64(None)),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_REF => Ok(ScalarValue::TimestampSecond(None, None)),
                TIMESTAMP_MILLI_TYPE_REF => {
                    Ok(ScalarValue::TimestampMillisecond(None, None))
                }
                TIMESTAMP_MICRO_TYPE_REF => {
                    Ok(ScalarValue::TimestampMicrosecond(None, None))
                }
                TIMESTAMP_NANO_TYPE_REF => {
                    Ok(ScalarValue::TimestampNanosecond(None, None))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_REF => Ok(ScalarValue::Date32(None)),
                DATE_64_TYPE_REF => Ok(ScalarValue::Date64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Binary(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeBinary(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            // FixedBinary is not supported because `None` doesn't have length
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_REF => Ok(ScalarValue::Utf8(None)),
                LARGE_CONTAINER_TYPE_REF => Ok(ScalarValue::LargeUtf8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            _ => not_impl_err!("Unsupported Substrait type: {kind:?}"),
        }
    } else {
        not_impl_err!("Null type without kind is not supported")
    }
}

fn from_substrait_field_reference(
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(StructField(x)) => match &x.child.as_ref() {
                Some(_) => not_impl_err!(
                    "Direct reference StructField with child is not supported"
                ),
                None => Ok(Expr::Column(Column::from(
                    input_schema.qualified_field(x.field as usize),
                ))),
            },
            _ => not_impl_err!(
                "Direct reference with types other than StructField is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}

/// Build [`Expr`] from its name and required inputs.
struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub async fn build(
        self,
        ctx: &SessionContext,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        match self.expr_name.as_str() {
            "like" => {
                Self::build_like_expr(ctx, false, f, input_schema, extensions).await
            }
            "ilike" => {
                Self::build_like_expr(ctx, true, f, input_schema, extensions).await
            }
            "not" | "negative" | "is_null" | "is_not_null" | "is_true" | "is_false"
            | "is_not_true" | "is_not_false" | "is_unknown" | "is_not_unknown" => {
                Self::build_unary_expr(ctx, &self.expr_name, f, input_schema, extensions)
                    .await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(
        ctx: &SessionContext,
        fn_name: &str,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        if f.arguments.len() != 1 {
            return substrait_err!("Expect one argument for {fn_name} expr");
        }
        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for {fn_name} expr");
        };
        let arg = from_substrait_rex(ctx, expr_substrait, input_schema, extensions)
            .await?
            .as_ref()
            .clone();
        let arg = Box::new(arg);

        let expr = match fn_name {
            "not" => Expr::Not(arg),
            "negative" => Expr::Negative(arg),
            "is_null" => Expr::IsNull(arg),
            "is_not_null" => Expr::IsNotNull(arg),
            "is_true" => Expr::IsTrue(arg),
            "is_false" => Expr::IsFalse(arg),
            "is_not_true" => Expr::IsNotTrue(arg),
            "is_not_false" => Expr::IsNotFalse(arg),
            "is_unknown" => Expr::IsUnknown(arg),
            "is_not_unknown" => Expr::IsNotUnknown(arg),
            _ => return not_impl_err!("Unsupported builtin expression: {}", fn_name),
        };

        Ok(Arc::new(expr))
    }

    async fn build_like_expr(
        ctx: &SessionContext,
        case_insensitive: bool,
        f: &ScalarFunction,
        input_schema: &DFSchema,
        extensions: &HashMap<u32, &String>,
    ) -> Result<Arc<Expr>> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if f.arguments.len() != 3 {
            return substrait_err!("Expect three arguments for `{fn_name}` expr");
        }

        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let expr = from_substrait_rex(ctx, expr_substrait, input_schema, extensions)
            .await?
            .as_ref()
            .clone();
        let Some(ArgType::Value(pattern_substrait)) = &f.arguments[1].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let pattern =
            from_substrait_rex(ctx, pattern_substrait, input_schema, extensions)
                .await?
                .as_ref()
                .clone();
        let Some(ArgType::Value(escape_char_substrait)) = &f.arguments[2].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let escape_char_expr =
            from_substrait_rex(ctx, escape_char_substrait, input_schema, extensions)
                .await?
                .as_ref()
                .clone();
        let Expr::Literal(ScalarValue::Utf8(escape_char)) = escape_char_expr else {
            return substrait_err!(
                "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
            );
        };

        Ok(Arc::new(Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char: escape_char.map(|c| c.chars().next().unwrap()),
            case_insensitive,
        })))
    }
}
