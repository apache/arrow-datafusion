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
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit
};
use datafusion::common::logical_type::field::LogicalField;
use datafusion::common::logical_type::schema::LogicalSchema;
use datafusion::common::logical_type::{TypeRelation, ExtensionType};
use datafusion::common::{
    not_impl_datafusion_err, not_impl_err, plan_datafusion_err, plan_err,
    substrait_datafusion_err, substrait_err, DFSchema, DFSchemaRef,
};
use substrait::proto::expression::literal::IntervalDayToSecond;
use substrait::proto::read_rel::local_files::file_or_files::PathType::UriFile;
use url::Url;

use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    aggregate_function, expr::find_df_window_func, Aggregate, BinaryExpr, Case,
    EmptyRelation, Expr, ExprSchemable, LogicalPlan, Operator, Projection, Values,
};

use datafusion::logical_expr::{
    col, expr, Cast, Extension, GroupingSet, Like, LogicalPlanBuilder, Partitioning,
    Repartition, Subquery, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
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
use substrait::proto::expression::literal::user_defined::Val;
use substrait::proto::expression::subquery::SubqueryType;
use substrait::proto::expression::{self, FieldReference, Literal, ScalarFunction};
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind, window_function::Bound,
        window_function::BoundsType, MaskExpression, RexType,
    },
    extensions::simple_extension_declaration::MappingType,
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    set_rel,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, Expression, NamedStruct, Plan, Rel, Type,
};
use substrait::proto::{FunctionArgument, SortField};

use datafusion::arrow::array::GenericListArray;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::logical_expr::expr::{InList, InSubquery, Sort};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_MONTH_DAY_NANO_TYPE_REF,
    INTERVAL_YEAR_MONTH_TYPE_REF, LARGE_CONTAINER_TYPE_VARIATION_REF,
    TIMESTAMP_MICRO_TYPE_VARIATION_REF, TIMESTAMP_MILLI_TYPE_VARIATION_REF,
    TIMESTAMP_NANO_TYPE_VARIATION_REF, TIMESTAMP_SECOND_TYPE_VARIATION_REF,
    UNSIGNED_INTEGER_TYPE_VARIATION_REF,
};

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

pub fn substrait_fun_name(name: &str) -> &str {
    let name = match name.rsplit_once(':') {
        // Since 0.32.0, Substrait requires the function names to be in a compound format
        // https://substrait.io/extensions/#function-signature-compound-names
        // for example, `add:i8_i8`.
        // On the consumer side, we don't really care about the signature though, just the name.
        Some((name, _)) => name,
        None => name,
    };
    name
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
                        let plan = from_substrait_rel(ctx, root.input.as_ref().unwrap(), &function_extension).await?;
                        if root.names.is_empty() {
                            // Backwards compatibility for plans missing names
                            return Ok(plan);
                        }
                        let renamed_schema = make_renamed_schema(plan.schema(), &root.names)?;
                        if renamed_schema.equivalent_names_and_types(plan.schema()) {
                            // Nothing to do if the schema is already equivalent
                            return Ok(plan);
                        }

                        match plan {
                            // If the last node of the plan produces expressions, bake the renames into those expressions.
                            // This isn't necessary for correctness, but helps with roundtrip tests.
                            LogicalPlan::Projection(p) => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(p.expr, p.input.schema(), &renamed_schema)?, p.input)?)),
                            LogicalPlan::Aggregate(a) => {
                                let new_aggr_exprs = rename_expressions(a.aggr_expr, a.input.schema(), &renamed_schema)?;
                                Ok(LogicalPlan::Aggregate(Aggregate::try_new(a.input, a.group_expr, new_aggr_exprs)?))
                            },
                            // There are probably more plans where we could bake things in, can add them later as needed.
                            // Otherwise, add a new Project to handle the renaming.
                            _ => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(plan.schema().columns().iter().map(|c| col(c.to_owned())), plan.schema(), &renamed_schema)?, Arc::new(plan))?))
                        }
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

/// parse projection
pub fn extract_projection(
    t: LogicalPlan,
    projection: &::core::option::Option<expression::MaskExpression>,
) -> Result<LogicalPlan> {
    match projection {
        Some(MaskExpression { select, .. }) => match &select.as_ref() {
            Some(projection) => {
                let column_indices: Vec<usize> = projection
                    .struct_items
                    .iter()
                    .map(|item| item.field as usize)
                    .collect();
                match t {
                    LogicalPlan::TableScan(mut scan) => {
                        let fields = column_indices
                            .iter()
                            .map(|i| scan.projected_schema.qualified_field(*i))
                            .map(|(qualifier, field)| {
                                (qualifier.cloned(), Arc::new(field.clone()))
                            })
                            .collect();
                        scan.projection = Some(column_indices);
                        scan.projected_schema = DFSchemaRef::new(
                            DFSchema::new_with_metadata(fields, HashMap::new())?,
                        );
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

/// Ensure the expressions have the right name(s) according to the new schema.
/// This includes the top-level (column) name, which will be renamed through aliasing if needed,
/// as well as nested names (if the expression produces any struct types), which will be renamed
/// through casting if needed.
fn rename_expressions(
    exprs: impl IntoIterator<Item = Expr>,
    input_schema: &DFSchema,
    new_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .zip(new_schema.fields())
        .map(|(old_expr, new_field)| {
            // Check if type (i.e. nested struct field names) match, use Cast to rename if needed
            let new_expr = if &old_expr.get_type(input_schema)? != new_field.data_type() {
                Expr::Cast(Cast::new(
                    Box::new(old_expr),
                    new_field.data_type().to_owned(),
                ))
            } else {
                old_expr
            };
            // Alias column if needed to fix the top-level name
            match &new_expr {
                // If expr is a column reference, alias_if_changed would cause an aliasing if the old expr has a qualifier
                Expr::Column(c) if &c.name == new_field.name() => Ok(new_expr),
                _ => new_expr.alias_if_changed(new_field.name().to_owned()),
            }
        })
        .collect()
}

/// Produce a version of the given schema with names matching the given list of names.
/// Substrait doesn't deal with column (incl. nested struct field) names within the schema,
/// but it does give us the list of expected names at the end of the plan, so we use this
/// to rename the schema to match the expected names.
fn make_renamed_schema(
    schema: &DFSchemaRef,
    dfs_names: &Vec<String>,
) -> Result<DFSchema> {
    fn rename_inner_fields(
        dtype: &DataType,
        dfs_names: &Vec<String>,
        name_idx: &mut usize,
    ) -> Result<DataType> {
        match dtype {
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| {
                        let name = next_struct_field_name(0, dfs_names, name_idx)?;
                        Ok((**f).to_owned().with_name(name).with_data_type(
                            rename_inner_fields(f.data_type(), dfs_names, name_idx)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                Ok(DataType::Struct(fields))
            }
            DataType::List(inner) => Ok(DataType::List(FieldRef::new(
                (**inner).to_owned().with_data_type(rename_inner_fields(
                    inner.data_type(),
                    dfs_names,
                    name_idx,
                )?),
            ))),
            DataType::LargeList(inner) => Ok(DataType::LargeList(FieldRef::new(
                (**inner).to_owned().with_data_type(rename_inner_fields(
                    inner.data_type(),
                    dfs_names,
                    name_idx,
                )?),
            ))),
            _ => Ok(dtype.to_owned()),
        }
    }

    let mut name_idx = 0;

    let (qualifiers, fields): (_, Vec<LogicalField>) = schema
        .iter()
        .map(|(q, f)| {
            let name = next_struct_field_name(0, dfs_names, &mut name_idx)?;
            Ok((
                q.cloned(),
                (**f)
                    .to_owned()
                    .with_name(name)
                    .with_data_type(rename_inner_fields(
                        f.data_type().physical(),
                        dfs_names,
                        &mut name_idx,
                    )?.into()),
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    if name_idx != dfs_names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            dfs_names.len());
    }

    DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(LogicalSchema::new(fields)),
    )
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
            let (left, right) = requalify_sides_if_needed(left, right)?;

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
            let left = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, cross.left.as_ref().unwrap(), extensions).await?,
            );
            let right = LogicalPlanBuilder::from(
                from_substrait_rel(ctx, cross.right.as_ref().unwrap(), extensions)
                    .await?,
            );
            let (left, right) = requalify_sides_if_needed(left, right)?;
            left.cross_join(right.build()?)?.build()
        }
        Some(RelType::Read(read)) => match &read.as_ref().read_type {
            Some(ReadType::NamedTable(nt)) => {
                let table_reference = match nt.names.len() {
                    0 => {
                        return plan_err!("No table name found in NamedTable");
                    }
                    1 => TableReference::Bare {
                        table: nt.names[0].clone().into(),
                    },
                    2 => TableReference::Partial {
                        schema: nt.names[0].clone().into(),
                        table: nt.names[1].clone().into(),
                    },
                    _ => TableReference::Full {
                        catalog: nt.names[0].clone().into(),
                        schema: nt.names[1].clone().into(),
                        table: nt.names[2].clone().into(),
                    },
                };
                let t = ctx.table(table_reference).await?;
                let t = t.into_optimized_plan()?;
                extract_projection(t, &read.projection)
            }
            Some(ReadType::VirtualTable(vt)) => {
                let base_schema = read.base_schema.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("No base schema provided for Virtual Table")
                })?;

                let schema = from_substrait_named_struct(base_schema)?;

                if vt.values.is_empty() {
                    return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                        produce_one_row: false,
                        schema,
                    }));
                }

                let values = vt
                    .values
                    .iter()
                    .map(|row| {
                        let mut name_idx = 0;
                        let lits = row
                            .fields
                            .iter()
                            .map(|lit| {
                                name_idx += 1; // top-level names are provided through schema
                                Ok(Expr::Literal(from_substrait_literal(
                                    lit,
                                    &base_schema.names,
                                    &mut name_idx,
                                )?))
                            })
                            .collect::<Result<_>>()?;
                        if name_idx != base_schema.names.len() {
                            return substrait_err!(
                                "Names list must match exactly to nested schema, but found {} uses for {} names",
                                name_idx,
                                base_schema.names.len()
                            );
                        }
                        Ok(lits)
                    })
                    .collect::<Result<_>>()?;

                Ok(LogicalPlan::Values(Values { schema, values }))
            }
            Some(ReadType::LocalFiles(lf)) => {
                fn extract_filename(name: &str) -> Option<String> {
                    let corrected_url =
                        if name.starts_with("file://") && !name.starts_with("file:///") {
                            name.replacen("file://", "file:///", 1)
                        } else {
                            name.to_string()
                        };

                    Url::parse(&corrected_url).ok().and_then(|url| {
                        let path = url.path();
                        std::path::Path::new(path)
                            .file_name()
                            .map(|filename| filename.to_string_lossy().to_string())
                    })
                }

                // we could use the file name to check the original table provider
                // TODO: currently does not support multiple local files
                let filename: Option<String> =
                    lf.items.first().and_then(|x| match x.path_type.as_ref() {
                        Some(UriFile(name)) => extract_filename(name),
                        _ => None,
                    });

                if lf.items.len() > 1 || filename.is_none() {
                    return not_impl_err!("Only single file reads are supported");
                }
                let name = filename.unwrap();
                // directly use unwrap here since we could determine it is a valid one
                let table_reference = TableReference::Bare { table: name.into() };
                let t = ctx.table(table_reference).await?;
                let t = t.into_optimized_plan()?;
                extract_projection(t, &read.projection)
            }
            _ => not_impl_err!("Unsupported ReadType: {:?}", &read.as_ref().read_type),
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
            let plan =
                plan.with_exprs_and_inputs(plan.expressions(), vec![input_plan])?;
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
            let plan = plan.with_exprs_and_inputs(plan.expressions(), inputs)?;
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

/// (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
/// conflict with the columns from the other.
/// Substrait doesn't currently allow specifying aliases, neither for columns nor for tables. For
/// Substrait the names don't matter since it only refers to columns by indices, however DataFusion
/// requires columns to be uniquely identifiable, in some places (see e.g. DFSchema::check_names).
fn requalify_sides_if_needed(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
) -> Result<(LogicalPlanBuilder, LogicalPlanBuilder)> {
    let left_cols = left.schema().columns();
    let right_cols = right.schema().columns();
    if left_cols.iter().any(|l| {
        right_cols.iter().any(|r| {
            l == r || (l.name == r.name && (l.relation.is_none() || r.relation.is_none()))
        })
    }) {
        // These names have no connection to the original plan, but they'll make the columns
        // (mostly) unique. There may be cases where this still causes duplicates, if either left
        // or right side itself contains duplicate names with different qualifiers.
        Ok((
            left.alias(TableReference::bare("left"))?,
            right.alias(TableReference::bare("right"))?,
        ))
    } else {
        Ok((left, right))
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
pub async fn from_substrait_func_args(
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
            _ => not_impl_err!("Function argument non-Value type not supported"),
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
    let args =
        from_substrait_func_args(ctx, &f.arguments, input_schema, extensions).await?;

    let Some(function_name) = extensions.get(&f.function_reference) else {
        return plan_err!(
            "Aggregate function not registered: function anchor = {:?}",
            f.function_reference
        );
    };

    let function_name = substrait_fun_name((**function_name).as_str());
    // try udaf first, then built-in aggr fn.
    if let Ok(fun) = ctx.udaf(function_name) {
        // deal with situation that count(*) got no arguments
        let args = if fun.name() == "count" && args.is_empty() {
            vec![Expr::Literal(ScalarValue::Int64(Some(1)))]
        } else {
            args
        };

        Ok(Arc::new(Expr::AggregateFunction(
            expr::AggregateFunction::new_udf(fun, args, distinct, filter, order_by, None),
        )))
    } else if let Ok(fun) = aggregate_function::AggregateFunction::from_str(function_name)
    {
        Ok(Arc::new(Expr::AggregateFunction(
            expr::AggregateFunction::new(fun, args, distinct, filter, order_by, None),
        )))
    } else {
        not_impl_err!(
            "Aggregate function {} is not supported: function anchor = {:?}",
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
            let Some(fn_name) = extensions.get(&f.function_reference) else {
                return plan_err!(
                    "Scalar function not found: function reference = {:?}",
                    f.function_reference
                );
            };
            let fn_name = substrait_fun_name(fn_name);

            let args =
                from_substrait_func_args(ctx, &f.arguments, input_schema, extensions)
                    .await?;

            // try to first match the requested function into registered udfs, then built-in ops
            // and finally built-in expressions
            if let Some(func) = ctx.state().scalar_functions().get(fn_name) {
                Ok(Arc::new(Expr::ScalarFunction(
                    expr::ScalarFunction::new_udf(func.to_owned(), args),
                )))
            } else if let Ok(op) = name_to_op(fn_name) {
                if args.len() != 2 {
                    return not_impl_err!(
                        "Expect two arguments for binary operator {op:?}"
                    );
                }

                Ok(Arc::new(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(args[0].to_owned()),
                    op,
                    right: Box::new(args[1].to_owned()),
                })))
            } else if let Some(builder) = BuiltinExprBuilder::try_from_name(fn_name) {
                builder.build(ctx, f, input_schema, extensions).await
            } else {
                not_impl_err!("Unsupported function name: {fn_name:?}")
            }
        }
        Some(RexType::Literal(lit)) => {
            let scalar_value = from_substrait_literal_without_names(lit)?;
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
                from_substrait_type_without_names(output_type)?,
            )))),
            None => substrait_err!("Cast expression without output type is not allowed"),
        },
        Some(RexType::WindowFunction(window)) => {
            let Some(fn_name) = extensions.get(&window.function_reference) else {
                return plan_err!(
                    "Window function not found: function reference = {:?}",
                    window.function_reference
                );
            };
            let fn_name = substrait_fun_name(fn_name);

            // check udaf first, then built-in functions
            let fun = match ctx.udaf(fn_name) {
                Ok(udaf) => Ok(WindowFunctionDefinition::AggregateUDF(udaf)),
                Err(_) => find_df_window_func(fn_name).ok_or_else(|| {
                    not_impl_datafusion_err!(
                        "Window function {} is not supported: function anchor = {:?}",
                        fn_name,
                        window.function_reference
                    )
                }),
            }?;

            let order_by =
                from_substrait_sorts(ctx, &window.sorts, input_schema, extensions)
                    .await?;

            let bound_units =
                match BoundsType::try_from(window.bounds_type).map_err(|e| {
                    plan_datafusion_err!("Invalid bound type {}: {e}", window.bounds_type)
                })? {
                    BoundsType::Rows => WindowFrameUnits::Rows,
                    BoundsType::Range => WindowFrameUnits::Range,
                    BoundsType::Unspecified => {
                        // If the plan does not specify the bounds type, then we use a simple logic to determine the units
                        // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
                        // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
                        if order_by.is_empty() {
                            WindowFrameUnits::Rows
                        } else {
                            WindowFrameUnits::Range
                        }
                    }
                };
            Ok(Arc::new(Expr::WindowFunction(expr::WindowFunction {
                fun,
                args: from_substrait_func_args(
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
                    bound_units,
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

pub(crate) fn from_substrait_type_without_names(dt: &Type) -> Result<TypeRelation> {
    from_substrait_type(dt, &[], &mut 0)
}

fn from_substrait_type(
    dt: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<TypeRelation> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean.into()),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int8.into()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt8.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int16.into()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt16.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int32.into()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt32.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int64.into()),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt64.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32.into()),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64.into()),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Second, None).into())
                }
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Millisecond, None).into())
                }
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Microsecond, None).into())
                }
                TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                    Ok(DataType::Timestamp(TimeUnit::Nanosecond, None).into())
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(DataType::Date32.into()),
                DATE_64_TYPE_VARIATION_REF => Ok(DataType::Date64.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary.into()),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length).into())
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8.into()),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8.into()),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::List(list) => {
                let inner_type = list.r#type.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("List type must have inner type")
                })?;
                let field = Arc::new(Field::new_list_field(
                    from_substrait_type(inner_type, dfs_names, name_idx)?.physical().clone(),
                    // We ignore Substrait's nullability here to match to_substrait_literal
                    // which always creates nullable lists
                    true,
                ));
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::List(field).into()),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeList(field).into()),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Map(map) => {
                let key_type = map.key.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have key type")
                })?;
                let value_type = map.value.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have value type")
                })?;
                let key_field = Arc::new(Field::new(
                    "key",
                    from_substrait_type(key_type, dfs_names, name_idx)?.physical().clone(),
                    false,
                ));
                let value_field = Arc::new(Field::new(
                    "value",
                    from_substrait_type(value_type, dfs_names, name_idx)?.physical().clone(),
                    true,
                ));
                match map.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => {
                        Ok(DataType::Map(Arc::new(Field::new_struct(
                            "entries",
                            [key_field, value_field],
                            false, // The inner map field is always non-nullable (Arrow #1697),
                        )), false).into())
                    },
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8).into())
                }
                DECIMAL_256_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8).into())
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::UserDefined(u) => {
                match u.type_reference {
                    INTERVAL_YEAR_MONTH_TYPE_REF => {
                        Ok(DataType::Interval(IntervalUnit::YearMonth).into())
                    }
                    INTERVAL_DAY_TIME_TYPE_REF => {
                        Ok(DataType::Interval(IntervalUnit::DayTime).into())
                    }
                    INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                        Ok(DataType::Interval(IntervalUnit::MonthDayNano).into())
                    }
                    _ => not_impl_err!(
                        "Unsupported Substrait user defined type with ref {} and variation {}",
                        u.type_reference,
                        u.type_variation_reference
                    ),
                }
            },
            r#type::Kind::Struct(s) => Ok(DataType::Struct(from_substrait_struct_type(
                s, dfs_names, name_idx,
            )?).into()),
            r#type::Kind::Varchar(_) => Ok(DataType::Utf8.into()),
            r#type::Kind::FixedChar(_) => Ok(DataType::Utf8.into()),
            _ => not_impl_err!("Unsupported Substrait type: {s_kind:?}"),
        },
        _ => not_impl_err!("`None` Substrait kind is not supported"),
    }
}

fn from_substrait_struct_type(
    s: &r#type::Struct,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<Fields> {
    let mut fields = vec![];
    for (i, f) in s.types.iter().enumerate() {
        let field = Field::new(
            next_struct_field_name(i, dfs_names, name_idx)?,
            from_substrait_type(f, dfs_names, name_idx)?.physical().clone(),
            true, // We assume everything to be nullable since that's easier than ensuring it matches
        );
        fields.push(field);
    }
    Ok(fields.into())
}

fn next_struct_field_name(
    i: usize,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<String> {
    if dfs_names.is_empty() {
        // If names are not given, create dummy names
        // c0, c1, ... align with e.g. SqlToRel::create_named_struct
        Ok(format!("c{i}"))
    } else {
        let name = dfs_names.get(*name_idx).cloned().ok_or_else(|| {
            substrait_datafusion_err!("Named schema must contain names for all fields")
        })?;
        *name_idx += 1;
        Ok(name)
    }
}

fn from_substrait_named_struct(base_schema: &NamedStruct) -> Result<DFSchemaRef> {
    let mut name_idx = 0;
    let fields = from_substrait_struct_type(
        base_schema.r#struct.as_ref().ok_or_else(|| {
            substrait_datafusion_err!("Named struct must contain a struct")
        })?,
        &base_schema.names,
        &mut name_idx,
    );
    if name_idx != base_schema.names.len() {
        return substrait_err!(
                                "Names list must match exactly to nested schema, but found {} uses for {} names",
                                name_idx,
                                base_schema.names.len()
                            );
    }
    Ok(DFSchemaRef::new(DFSchema::try_from(Schema::new(fields?))?))
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

pub(crate) fn from_substrait_literal_without_names(lit: &Literal) -> Result<ScalarValue> {
    from_substrait_literal(lit, &vec![], &mut 0)
}

fn from_substrait_literal(
    lit: &Literal,
    dfs_names: &Vec<String>,
    name_idx: &mut usize,
) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => match lit.type_variation_reference {
            TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                ScalarValue::TimestampSecond(Some(*t), None)
            }
            TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                ScalarValue::TimestampMillisecond(Some(*t), None)
            }
            TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                ScalarValue::TimestampMicrosecond(Some(*t), None)
            }
            TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                ScalarValue::TimestampNanosecond(Some(*t), None)
            }
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => {
                ScalarValue::LargeBinary(Some(b.clone()))
            }
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
        Some(LiteralType::List(l)) => {
            let elements = l
                .values
                .iter()
                .map(|el| from_substrait_literal(el, dfs_names, name_idx))
                .collect::<Result<Vec<_>>>()?;
            if elements.is_empty() {
                return substrait_err!(
                    "Empty list must be encoded as EmptyList literal type, not List"
                );
            }
            let element_type = elements[0].data_type();
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::List(
                    ScalarValue::new_list_nullable(elements.as_slice(), &element_type),
                ),
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(elements.as_slice(), &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::EmptyList(l)) => {
            let element_type = from_substrait_type(
                l.r#type.clone().unwrap().as_ref(),
                dfs_names,
                name_idx,
            )?;
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => {
                    ScalarValue::List(ScalarValue::new_list_nullable(&[], &element_type.physical()))
                }
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(&[], &element_type.physical()),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::Struct(s)) => {
            let mut builder = ScalarStructBuilder::new();
            for (i, field) in s.fields.iter().enumerate() {
                let name = next_struct_field_name(i, dfs_names, name_idx)?;
                let sv = from_substrait_literal(field, dfs_names, name_idx)?;
                // We assume everything to be nullable, since Arrow's strict about things matching
                // and it's hard to match otherwise.
                builder = builder.with_scalar(Field::new(name, sv.data_type(), true), sv);
            }
            builder.build()?
        }
        Some(LiteralType::Null(ntype)) => {
            from_substrait_null(ntype, dfs_names, name_idx)?
        }
        Some(LiteralType::IntervalDayToSecond(IntervalDayToSecond {
            days,
            seconds,
            microseconds,
        })) => {
            ScalarValue::new_interval_dt(*days, (seconds * 1000) + (microseconds / 1000))
        }
        Some(LiteralType::UserDefined(user_defined)) => {
            match user_defined.type_reference {
                INTERVAL_YEAR_MONTH_TYPE_REF => {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval year month value is empty");
                    };
                    let value_slice: [u8; 4] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval year month value"
                            )
                        })?;
                    ScalarValue::IntervalYearMonth(Some(i32::from_le_bytes(value_slice)))
                }
                INTERVAL_DAY_TIME_TYPE_REF => {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval day time value is empty");
                    };
                    let value_slice: [u8; 8] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval day time value"
                            )
                        })?;
                    let days = i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                    let milliseconds =
                        i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                    ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                        days,
                        milliseconds,
                    }))
                }
                INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval month day nano value is empty");
                    };
                    let value_slice: [u8; 16] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval month day nano value"
                            )
                        })?;
                    let months =
                        i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                    let days = i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                    let nanoseconds =
                        i64::from_le_bytes(value_slice[8..16].try_into().unwrap());
                    ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                        months,
                        days,
                        nanoseconds,
                    }))
                }
                _ => {
                    return not_impl_err!(
                        "Unsupported Substrait user defined type with ref {}",
                        user_defined.type_reference
                    )
                }
            }
        }
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

fn from_substrait_null(
    null_type: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<ScalarValue> {
    if let Some(kind) = &null_type.kind {
        match kind {
            r#type::Kind::Bool(_) => Ok(ScalarValue::Boolean(None)),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int8(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int16(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt16(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int32(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt32(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(ScalarValue::Int64(None)),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(ScalarValue::UInt64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(ScalarValue::Float32(None)),
            r#type::Kind::Fp64(_) => Ok(ScalarValue::Float64(None)),
            r#type::Kind::Timestamp(ts) => match ts.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                    Ok(ScalarValue::TimestampSecond(None, None))
                }
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                    Ok(ScalarValue::TimestampMillisecond(None, None))
                }
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                    Ok(ScalarValue::TimestampMicrosecond(None, None))
                }
                TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                    Ok(ScalarValue::TimestampNanosecond(None, None))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(ScalarValue::Date32(None)),
                DATE_64_TYPE_VARIATION_REF => Ok(ScalarValue::Date64(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::Binary(None)),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeBinary(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            // FixedBinary is not supported because `None` doesn't have length
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::Utf8(None)),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeUtf8(None)),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {kind:?}"
                ),
            },
            r#type::Kind::Decimal(d) => Ok(ScalarValue::Decimal128(
                None,
                d.precision as u8,
                d.scale as i8,
            )),
            r#type::Kind::List(l) => {
                let field = LogicalField::new_list_field(
                    from_substrait_type(
                        l.r#type.clone().unwrap().as_ref(),
                        dfs_names,
                        name_idx,
                    )?,
                    true,
                );
                match l.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::List(
                        Arc::new(GenericListArray::new_null(FieldRef::new(field.into()), 1)),
                    )),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(ScalarValue::LargeList(
                        Arc::new(GenericListArray::new_null(FieldRef::new(field.into()), 1)),
                    )),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {kind:?}"
                    ),
                }
            }
            r#type::Kind::Struct(s) => {
                let fields: Fields = from_substrait_struct_type(s, dfs_names, name_idx)?.into();
                Ok(ScalarStructBuilder::new_null(fields))
            }
            _ => not_impl_err!("Unsupported Substrait type for null: {kind:?}"),
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
