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

use std::sync::Arc;

use datafusion::execution::registry::FunctionRegistry;
use datafusion_common::{
    internal_err, plan_datafusion_err, DataFusionError, Result, ScalarValue,
    TableReference, UnnestOptions,
};
use datafusion_expr::expr::Unnest;
use datafusion_expr::expr::{Alias, Placeholder};
use datafusion_expr::window_frame::{check_window_frame, regularize_window_order_by};
use datafusion_expr::{
    expr::{self, InList, Sort, WindowFunction},
    logical_plan::{PlanType, StringifiedPlan},
    AggregateFunction, Between, BinaryExpr, BuiltInWindowFunction, Case, Cast, Expr,
    GroupingSet,
    GroupingSet::GroupingSets,
    JoinConstraint, JoinType, Like, Operator, TryCast, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};
use datafusion_proto_common::{from_proto::FromOptionalField, FromProtoError as Error};

use crate::protobuf::{
    self,
    plan_type::PlanTypeEnum::{
        AnalyzedLogicalPlan, FinalAnalyzedLogicalPlan, FinalLogicalPlan,
        FinalPhysicalPlan, FinalPhysicalPlanWithStats, InitialLogicalPlan,
        InitialPhysicalPlan, InitialPhysicalPlanWithStats, OptimizedLogicalPlan,
        OptimizedPhysicalPlan,
    },
    AnalyzedLogicalPlanType, CubeNode, GroupingSetNode, OptimizedLogicalPlanType,
    OptimizedPhysicalPlanType, PlaceholderNode, RollupNode,
};

use super::LogicalExtensionCodec;

impl From<&protobuf::UnnestOptions> for UnnestOptions {
    fn from(opts: &protobuf::UnnestOptions) -> Self {
        Self {
            preserve_nulls: opts.preserve_nulls,
        }
    }
}

impl From<protobuf::WindowFrameUnits> for WindowFrameUnits {
    fn from(units: protobuf::WindowFrameUnits) -> Self {
        match units {
            protobuf::WindowFrameUnits::Rows => Self::Rows,
            protobuf::WindowFrameUnits::Range => Self::Range,
            protobuf::WindowFrameUnits::Groups => Self::Groups,
        }
    }
}

impl TryFrom<protobuf::TableReference> for TableReference {
    type Error = Error;

    fn try_from(value: protobuf::TableReference) -> Result<Self, Self::Error> {
        use protobuf::table_reference::TableReferenceEnum;
        let table_reference_enum = value
            .table_reference_enum
            .ok_or_else(|| Error::required("table_reference_enum"))?;

        match table_reference_enum {
            TableReferenceEnum::Bare(protobuf::BareTableReference { table }) => {
                Ok(TableReference::bare(table))
            }
            TableReferenceEnum::Partial(protobuf::PartialTableReference {
                schema,
                table,
            }) => Ok(TableReference::partial(schema, table)),
            TableReferenceEnum::Full(protobuf::FullTableReference {
                catalog,
                schema,
                table,
            }) => Ok(TableReference::full(catalog, schema, table)),
        }
    }
}

impl From<&protobuf::StringifiedPlan> for StringifiedPlan {
    fn from(stringified_plan: &protobuf::StringifiedPlan) -> Self {
        Self {
            plan_type: match stringified_plan
                .plan_type
                .as_ref()
                .and_then(|pt| pt.plan_type_enum.as_ref())
                .unwrap_or_else(|| {
                    panic!(
                        "Cannot create protobuf::StringifiedPlan from {stringified_plan:?}"
                    )
                }) {
                InitialLogicalPlan(_) => PlanType::InitialLogicalPlan,
                AnalyzedLogicalPlan(AnalyzedLogicalPlanType { analyzer_name }) => {
                    PlanType::AnalyzedLogicalPlan {
                        analyzer_name:analyzer_name.clone()
                    }
                }
                FinalAnalyzedLogicalPlan(_) => PlanType::FinalAnalyzedLogicalPlan,
                OptimizedLogicalPlan(OptimizedLogicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedLogicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalLogicalPlan(_) => PlanType::FinalLogicalPlan,
                InitialPhysicalPlan(_) => PlanType::InitialPhysicalPlan,
                InitialPhysicalPlanWithStats(_) => PlanType::InitialPhysicalPlanWithStats,
                OptimizedPhysicalPlan(OptimizedPhysicalPlanType { optimizer_name }) => {
                    PlanType::OptimizedPhysicalPlan {
                        optimizer_name: optimizer_name.clone(),
                    }
                }
                FinalPhysicalPlan(_) => PlanType::FinalPhysicalPlan,
                FinalPhysicalPlanWithStats(_) => PlanType::FinalPhysicalPlanWithStats,
            },
            plan: Arc::new(stringified_plan.plan.clone()),
        }
    }
}

impl From<protobuf::AggregateFunction> for AggregateFunction {
    fn from(agg_fun: protobuf::AggregateFunction) -> Self {
        match agg_fun {
            protobuf::AggregateFunction::Min => Self::Min,
            protobuf::AggregateFunction::Max => Self::Max,
            protobuf::AggregateFunction::Avg => Self::Avg,
            protobuf::AggregateFunction::ArrayAgg => Self::ArrayAgg,
            protobuf::AggregateFunction::Correlation => Self::Correlation,
            protobuf::AggregateFunction::Grouping => Self::Grouping,
            protobuf::AggregateFunction::NthValueAgg => Self::NthValue,
        }
    }
}

impl From<protobuf::BuiltInWindowFunction> for BuiltInWindowFunction {
    fn from(built_in_function: protobuf::BuiltInWindowFunction) -> Self {
        match built_in_function {
            protobuf::BuiltInWindowFunction::RowNumber => Self::RowNumber,
            protobuf::BuiltInWindowFunction::Rank => Self::Rank,
            protobuf::BuiltInWindowFunction::PercentRank => Self::PercentRank,
            protobuf::BuiltInWindowFunction::DenseRank => Self::DenseRank,
            protobuf::BuiltInWindowFunction::Lag => Self::Lag,
            protobuf::BuiltInWindowFunction::Lead => Self::Lead,
            protobuf::BuiltInWindowFunction::FirstValue => Self::FirstValue,
            protobuf::BuiltInWindowFunction::CumeDist => Self::CumeDist,
            protobuf::BuiltInWindowFunction::Ntile => Self::Ntile,
            protobuf::BuiltInWindowFunction::NthValue => Self::NthValue,
            protobuf::BuiltInWindowFunction::LastValue => Self::LastValue,
        }
    }
}

impl TryFrom<protobuf::WindowFrame> for WindowFrame {
    type Error = Error;

    fn try_from(window: protobuf::WindowFrame) -> Result<Self, Self::Error> {
        let units = protobuf::WindowFrameUnits::try_from(window.window_frame_units)
            .map_err(|_| Error::unknown("WindowFrameUnits", window.window_frame_units))?
            .into();
        let start_bound = window.start_bound.required("start_bound")?;
        let end_bound = window
            .end_bound
            .map(|end_bound| match end_bound {
                protobuf::window_frame::EndBound::Bound(end_bound) => {
                    end_bound.try_into()
                }
            })
            .transpose()?
            .unwrap_or(WindowFrameBound::CurrentRow);
        Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
    }
}

impl TryFrom<protobuf::WindowFrameBound> for WindowFrameBound {
    type Error = Error;

    fn try_from(bound: protobuf::WindowFrameBound) -> Result<Self, Self::Error> {
        let bound_type =
            protobuf::WindowFrameBoundType::try_from(bound.window_frame_bound_type)
                .map_err(|_| {
                    Error::unknown("WindowFrameBoundType", bound.window_frame_bound_type)
                })?;
        match bound_type {
            protobuf::WindowFrameBoundType::CurrentRow => Ok(Self::CurrentRow),
            protobuf::WindowFrameBoundType::Preceding => match bound.bound_value {
                Some(x) => Ok(Self::Preceding(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Preceding(ScalarValue::UInt64(None))),
            },
            protobuf::WindowFrameBoundType::Following => match bound.bound_value {
                Some(x) => Ok(Self::Following(ScalarValue::try_from(&x)?)),
                None => Ok(Self::Following(ScalarValue::UInt64(None))),
            },
        }
    }
}

impl From<protobuf::JoinType> for JoinType {
    fn from(t: protobuf::JoinType) -> Self {
        match t {
            protobuf::JoinType::Inner => JoinType::Inner,
            protobuf::JoinType::Left => JoinType::Left,
            protobuf::JoinType::Right => JoinType::Right,
            protobuf::JoinType::Full => JoinType::Full,
            protobuf::JoinType::Leftsemi => JoinType::LeftSemi,
            protobuf::JoinType::Rightsemi => JoinType::RightSemi,
            protobuf::JoinType::Leftanti => JoinType::LeftAnti,
            protobuf::JoinType::Rightanti => JoinType::RightAnti,
        }
    }
}

impl From<protobuf::JoinConstraint> for JoinConstraint {
    fn from(t: protobuf::JoinConstraint) -> Self {
        match t {
            protobuf::JoinConstraint::On => JoinConstraint::On,
            protobuf::JoinConstraint::Using => JoinConstraint::Using,
        }
    }
}

pub fn parse_i32_to_aggregate_function(value: &i32) -> Result<AggregateFunction, Error> {
    protobuf::AggregateFunction::try_from(*value)
        .map(|a| a.into())
        .map_err(|_| Error::unknown("AggregateFunction", *value))
}

pub fn parse_expr(
    proto: &protobuf::LogicalExprNode,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    use protobuf::{logical_expr_node::ExprType, window_expr_node};

    let expr_type = proto
        .expr_type
        .as_ref()
        .ok_or_else(|| Error::required("expr_type"))?;

    match expr_type {
        ExprType::BinaryExpr(binary_expr) => {
            let op = from_proto_binary_op(&binary_expr.op)?;
            let operands = parse_exprs(&binary_expr.operands, registry, codec)?;

            if operands.len() < 2 {
                return Err(proto_error(
                    "A binary expression must always have at least 2 operands",
                ));
            }

            // Reduce the linearized operands (ordered by left innermost to right
            // outermost) into a single expression tree.
            Ok(operands
                .into_iter()
                .reduce(|left, right| {
                    Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)))
                })
                .expect("Binary expression could not be reduced to a single expression."))
        }
        ExprType::Column(column) => Ok(Expr::Column(column.into())),
        ExprType::Literal(literal) => {
            let scalar_value: ScalarValue = literal.try_into()?;
            Ok(Expr::Literal(scalar_value))
        }
        ExprType::WindowExpr(expr) => {
            let window_function = expr
                .window_function
                .as_ref()
                .ok_or_else(|| Error::required("window_function"))?;
            let partition_by = parse_exprs(&expr.partition_by, registry, codec)?;
            let mut order_by = parse_exprs(&expr.order_by, registry, codec)?;
            let window_frame = expr
                .window_frame
                .as_ref()
                .map::<Result<WindowFrame, _>, _>(|window_frame| {
                    let window_frame = window_frame.clone().try_into()?;
                    check_window_frame(&window_frame, order_by.len())
                        .map(|_| window_frame)
                })
                .transpose()?
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "missing window frame during deserialization".to_string(),
                    )
                })?;
            // TODO: support proto for null treatment
            let null_treatment = None;
            regularize_window_order_by(&window_frame, &mut order_by)?;

            match window_function {
                window_expr_node::WindowFunction::AggrFunction(i) => {
                    let aggr_function = parse_i32_to_aggregate_function(i)?;

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::AggregateFunction(
                            aggr_function,
                        ),
                        vec![parse_required_expr(expr.expr.as_deref(), registry, "expr", codec)?],
                        partition_by,
                        order_by,
                        window_frame,
                        None
                    )))
                }
                window_expr_node::WindowFunction::BuiltInFunction(i) => {
                    let built_in_function = protobuf::BuiltInWindowFunction::try_from(*i)
                        .map_err(|_| Error::unknown("BuiltInWindowFunction", *i))?
                        .into();

                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);

                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::BuiltInWindowFunction(
                            built_in_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        null_treatment
                    )))
                }
                window_expr_node::WindowFunction::Udaf(udaf_name) => {
                    let udaf_function = registry.udaf(udaf_name)?;
                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);
                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::AggregateUDF(
                            udaf_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        None,
                    )))
                }
                window_expr_node::WindowFunction::Udwf(udwf_name) => {
                    let udwf_function = registry.udwf(udwf_name)?;
                    let args =
                        parse_optional_expr(expr.expr.as_deref(), registry, codec)?
                            .map(|e| vec![e])
                            .unwrap_or_else(Vec::new);
                    Ok(Expr::WindowFunction(WindowFunction::new(
                        datafusion_expr::expr::WindowFunctionDefinition::WindowUDF(
                            udwf_function,
                        ),
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        None,
                    )))
                }
            }
        }
        ExprType::AggregateExpr(expr) => {
            let fun = parse_i32_to_aggregate_function(&expr.aggr_function)?;

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun,
                parse_exprs(&expr.expr, registry, codec)?,
                expr.distinct,
                parse_optional_expr(expr.filter.as_deref(), registry, codec)?
                    .map(Box::new),
                parse_vec_expr(&expr.order_by, registry, codec)?,
                None,
            )))
        }
        ExprType::Alias(alias) => Ok(Expr::Alias(Alias::new(
            parse_required_expr(alias.expr.as_deref(), registry, "expr", codec)?,
            alias
                .relation
                .first()
                .map(|r| TableReference::try_from(r.clone()))
                .transpose()?,
            alias.alias.clone(),
        ))),
        ExprType::IsNullExpr(is_null) => Ok(Expr::IsNull(Box::new(parse_required_expr(
            is_null.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotNullExpr(is_not_null) => Ok(Expr::IsNotNull(Box::new(
            parse_required_expr(is_not_null.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::NotExpr(not) => Ok(Expr::Not(Box::new(parse_required_expr(
            not.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsTrue(msg) => Ok(Expr::IsTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsFalse(msg) => Ok(Expr::IsFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsUnknown(msg) => Ok(Expr::IsUnknown(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotTrue(msg) => Ok(Expr::IsNotTrue(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotFalse(msg) => Ok(Expr::IsNotFalse(Box::new(parse_required_expr(
            msg.expr.as_deref(),
            registry,
            "expr",
            codec,
        )?))),
        ExprType::IsNotUnknown(msg) => Ok(Expr::IsNotUnknown(Box::new(
            parse_required_expr(msg.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Between(between) => Ok(Expr::Between(Between::new(
            Box::new(parse_required_expr(
                between.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            between.negated,
            Box::new(parse_required_expr(
                between.low.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                between.high.as_deref(),
                registry,
                "expr",
                codec,
            )?),
        ))),
        ExprType::Like(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Ilike(like) => Ok(Expr::Like(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            true,
        ))),
        ExprType::SimilarTo(like) => Ok(Expr::SimilarTo(Like::new(
            like.negated,
            Box::new(parse_required_expr(
                like.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            Box::new(parse_required_expr(
                like.pattern.as_deref(),
                registry,
                "pattern",
                codec,
            )?),
            parse_escape_char(&like.escape_char)?,
            false,
        ))),
        ExprType::Case(case) => {
            let when_then_expr = case
                .when_then_expr
                .iter()
                .map(|e| {
                    let when_expr = parse_required_expr(
                        e.when_expr.as_ref(),
                        registry,
                        "when_expr",
                        codec,
                    )?;
                    let then_expr = parse_required_expr(
                        e.then_expr.as_ref(),
                        registry,
                        "then_expr",
                        codec,
                    )?;
                    Ok((Box::new(when_expr), Box::new(then_expr)))
                })
                .collect::<Result<Vec<(Box<Expr>, Box<Expr>)>, Error>>()?;
            Ok(Expr::Case(Case::new(
                parse_optional_expr(case.expr.as_deref(), registry, codec)?.map(Box::new),
                when_then_expr,
                parse_optional_expr(case.else_expr.as_deref(), registry, codec)?
                    .map(Box::new),
            )))
        }
        ExprType::Cast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::Cast(Cast::new(expr, data_type)))
        }
        ExprType::TryCast(cast) => {
            let expr = Box::new(parse_required_expr(
                cast.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?);
            let data_type = cast.arrow_type.as_ref().required("arrow_type")?;
            Ok(Expr::TryCast(TryCast::new(expr, data_type)))
        }
        ExprType::Sort(sort) => Ok(Expr::Sort(Sort::new(
            Box::new(parse_required_expr(
                sort.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            sort.asc,
            sort.nulls_first,
        ))),
        ExprType::Negative(negative) => Ok(Expr::Negative(Box::new(
            parse_required_expr(negative.expr.as_deref(), registry, "expr", codec)?,
        ))),
        ExprType::Unnest(unnest) => {
            let mut exprs = parse_exprs(&unnest.exprs, registry, codec)?;
            if exprs.len() != 1 {
                return Err(proto_error("Unnest must have exactly one expression"));
            }
            Ok(Expr::Unnest(Unnest::new(exprs.swap_remove(0))))
        }
        ExprType::InList(in_list) => Ok(Expr::InList(InList::new(
            Box::new(parse_required_expr(
                in_list.expr.as_deref(),
                registry,
                "expr",
                codec,
            )?),
            parse_exprs(&in_list.list, registry, codec)?,
            in_list.negated,
        ))),
        ExprType::Wildcard(protobuf::Wildcard { qualifier }) => Ok(Expr::Wildcard {
            qualifier: if qualifier.is_empty() {
                None
            } else {
                Some(qualifier.clone())
            },
        }),
        ExprType::ScalarUdfExpr(protobuf::ScalarUdfExprNode {
            fun_name,
            args,
            fun_definition,
        }) => {
            let scalar_fn = match fun_definition {
                Some(buf) => codec.try_decode_udf(fun_name, buf)?,
                None => registry.udf(fun_name.as_str())?,
            };
            Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
                scalar_fn,
                parse_exprs(args, registry, codec)?,
            )))
        }
        ExprType::AggregateUdfExpr(pb) => {
            let agg_fn = registry.udaf(pb.fun_name.as_str())?;

            Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                agg_fn,
                parse_exprs(&pb.args, registry, codec)?,
                pb.distinct,
                parse_optional_expr(pb.filter.as_deref(), registry, codec)?.map(Box::new),
                parse_vec_expr(&pb.order_by, registry, codec)?,
                None,
            )))
        }

        ExprType::GroupingSet(GroupingSetNode { expr }) => {
            Ok(Expr::GroupingSet(GroupingSets(
                expr.iter()
                    .map(|expr_list| parse_exprs(&expr_list.expr, registry, codec))
                    .collect::<Result<Vec<_>, Error>>()?,
            )))
        }
        ExprType::Cube(CubeNode { expr }) => Ok(Expr::GroupingSet(GroupingSet::Cube(
            parse_exprs(expr, registry, codec)?,
        ))),
        ExprType::Rollup(RollupNode { expr }) => Ok(Expr::GroupingSet(
            GroupingSet::Rollup(parse_exprs(expr, registry, codec)?),
        )),
        ExprType::Placeholder(PlaceholderNode { id, data_type }) => match data_type {
            None => Ok(Expr::Placeholder(Placeholder::new(id.clone(), None))),
            Some(data_type) => Ok(Expr::Placeholder(Placeholder::new(
                id.clone(),
                Some(data_type.try_into()?),
            ))),
        },
    }
}

/// Parse a vector of `protobuf::LogicalExprNode`s.
pub fn parse_exprs<'a, I>(
    protos: I,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Vec<Expr>, Error>
where
    I: IntoIterator<Item = &'a protobuf::LogicalExprNode>,
{
    let res = protos
        .into_iter()
        .map(|elem| {
            parse_expr(elem, registry, codec).map_err(|e| plan_datafusion_err!("{}", e))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(res)
}

/// Parse an optional escape_char for Like, ILike, SimilarTo
fn parse_escape_char(s: &str) -> Result<Option<char>> {
    match s.len() {
        0 => Ok(None),
        1 => Ok(s.chars().next()),
        _ => internal_err!("Invalid length for escape char"),
    }
}

pub fn from_proto_binary_op(op: &str) -> Result<Operator, Error> {
    match op {
        "And" => Ok(Operator::And),
        "Or" => Ok(Operator::Or),
        "Eq" => Ok(Operator::Eq),
        "NotEq" => Ok(Operator::NotEq),
        "LtEq" => Ok(Operator::LtEq),
        "Lt" => Ok(Operator::Lt),
        "Gt" => Ok(Operator::Gt),
        "GtEq" => Ok(Operator::GtEq),
        "Plus" => Ok(Operator::Plus),
        "Minus" => Ok(Operator::Minus),
        "Multiply" => Ok(Operator::Multiply),
        "Divide" => Ok(Operator::Divide),
        "Modulo" => Ok(Operator::Modulo),
        "IsDistinctFrom" => Ok(Operator::IsDistinctFrom),
        "IsNotDistinctFrom" => Ok(Operator::IsNotDistinctFrom),
        "BitwiseAnd" => Ok(Operator::BitwiseAnd),
        "BitwiseOr" => Ok(Operator::BitwiseOr),
        "BitwiseXor" => Ok(Operator::BitwiseXor),
        "BitwiseShiftLeft" => Ok(Operator::BitwiseShiftLeft),
        "BitwiseShiftRight" => Ok(Operator::BitwiseShiftRight),
        "RegexIMatch" => Ok(Operator::RegexIMatch),
        "RegexMatch" => Ok(Operator::RegexMatch),
        "RegexNotIMatch" => Ok(Operator::RegexNotIMatch),
        "RegexNotMatch" => Ok(Operator::RegexNotMatch),
        "StringConcat" => Ok(Operator::StringConcat),
        "AtArrow" => Ok(Operator::AtArrow),
        "ArrowAt" => Ok(Operator::ArrowAt),
        other => Err(proto_error(format!(
            "Unsupported binary operator '{other:?}'"
        ))),
    }
}

fn parse_vec_expr(
    p: &[protobuf::LogicalExprNode],
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Option<Vec<Expr>>, Error> {
    let res = parse_exprs(p, registry, codec)?;
    // Convert empty vector to None.
    Ok((!res.is_empty()).then_some(res))
}

fn parse_optional_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Option<Expr>, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec).map(Some),
        None => Ok(None),
    }
}

fn parse_required_expr(
    p: Option<&protobuf::LogicalExprNode>,
    registry: &dyn FunctionRegistry,
    field: impl Into<String>,
    codec: &dyn LogicalExtensionCodec,
) -> Result<Expr, Error> {
    match p {
        Some(expr) => parse_expr(expr, registry, codec),
        None => Err(Error::required(field)),
    }
}

fn proto_error<S: Into<String>>(message: S) -> Error {
    Error::General(message.into())
}
