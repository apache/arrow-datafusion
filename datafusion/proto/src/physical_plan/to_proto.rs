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
// under the License.language governing permissions and
// limitations under the License.

use std::{
    convert::{TryFrom, TryInto},
    str::FromStr,
    sync::Arc,
};

use crate::protobuf::{
    self, copy_to_node, physical_aggregate_expr_node, physical_window_expr_node,
    scalar_value::Value, ArrowOptions, AvroOptions, PhysicalSortExprNode,
    PhysicalSortExprNodeCollection, ScalarValue,
};

#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;

use datafusion_expr::ScalarFunctionDefinition;

use crate::logical_plan::csv_writer_options_to_proto;
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::physical_expr::window::{NthValueKind, SlidingAggregateWindowExpr};
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{
    ApproxDistinct, ApproxMedian, ApproxPercentileCont, ApproxPercentileContWithWeight,
    ArrayAgg, Avg, BinaryExpr, BitAnd, BitOr, BitXor, BoolAnd, BoolOr, CaseExpr,
    CastExpr, Column, Correlation, Count, Covariance, CovariancePop, CumeDist,
    DistinctArrayAgg, DistinctBitXor, DistinctCount, DistinctSum, FirstValue, Grouping,
    InListExpr, IsNotNullExpr, IsNullExpr, LastValue, Literal, Max, Median, Min,
    NegativeExpr, NotExpr, NthValue, NthValueAgg, Ntile, OrderSensitiveArrayAgg, Rank,
    RankType, Regr, RegrType, RowNumber, Stddev, StddevPop, StringAgg, Sum, TryCastExpr,
    Variance, VariancePop, WindowShift,
};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::windows::{BuiltInWindowExpr, PlainAggregateWindowExpr};
use datafusion::physical_plan::{
    AggregateExpr, ColumnStatistics, PhysicalExpr, Statistics, WindowExpr,
};
use datafusion::{
    datasource::{
        file_format::{csv::CsvSink, json::JsonSink},
        listing::{FileRange, PartitionedFile},
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    physical_plan::expressions::LikeExpr,
};
use datafusion_common::config::{
    ColumnOptions, CsvOptions, FormatOptions, JsonOptions, ParquetOptions,
    TableParquetOptions,
};
use datafusion_common::{
    file_options::{csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions},
    internal_err, not_impl_err,
    parsers::CompressionTypeVariant,
    stats::Precision,
    DataFusionError, JoinSide, Result,
};

use super::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};

impl TryFrom<Arc<dyn AggregateExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(a: Arc<dyn AggregateExpr>) -> Result<Self, Self::Error> {
        let codec = DefaultPhysicalExtensionCodec {};
        let expressions = serialize_physical_exprs(a.expressions(), &codec)?;

        let ordering_req = a.order_bys().unwrap_or(&[]).to_vec();
        let ordering_req = serialize_physical_sort_exprs(ordering_req, &codec)?;

        if let Some(a) = a.as_any().downcast_ref::<AggregateFunctionExpr>() {
            let name = a.fun().name().to_string();
            return Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                        protobuf::PhysicalAggregateExprNode {
                            aggregate_function: Some(physical_aggregate_expr_node::AggregateFunction::UserDefinedAggrFunction(name)),
                            expr: expressions,
                            ordering_req,
                            distinct: false,
                        },
                    )),
                });
        }

        let AggrFn {
            inner: aggr_function,
            distinct,
        } = aggr_expr_to_aggr_fn(a.as_ref())?;

        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::AggregateExpr(
                protobuf::PhysicalAggregateExprNode {
                    aggregate_function: Some(
                        physical_aggregate_expr_node::AggregateFunction::AggrFunction(
                            aggr_function as i32,
                        ),
                    ),
                    expr: expressions,
                    ordering_req,
                    distinct,
                },
            )),
        })
    }
}

impl TryFrom<Arc<dyn WindowExpr>> for protobuf::PhysicalWindowExprNode {
    type Error = DataFusionError;

    fn try_from(
        window_expr: Arc<dyn WindowExpr>,
    ) -> std::result::Result<Self, Self::Error> {
        let expr = window_expr.as_any();

        let mut args = window_expr.expressions().to_vec();
        let window_frame = window_expr.get_window_frame();

        let window_function = if let Some(built_in_window_expr) =
            expr.downcast_ref::<BuiltInWindowExpr>()
        {
            let expr = built_in_window_expr.get_built_in_func_expr();
            let built_in_fn_expr = expr.as_any();

            let builtin_fn = if built_in_fn_expr.downcast_ref::<RowNumber>().is_some() {
                protobuf::BuiltInWindowFunction::RowNumber
            } else if let Some(rank_expr) = built_in_fn_expr.downcast_ref::<Rank>() {
                match rank_expr.get_type() {
                    RankType::Basic => protobuf::BuiltInWindowFunction::Rank,
                    RankType::Dense => protobuf::BuiltInWindowFunction::DenseRank,
                    RankType::Percent => protobuf::BuiltInWindowFunction::PercentRank,
                }
            } else if built_in_fn_expr.downcast_ref::<CumeDist>().is_some() {
                protobuf::BuiltInWindowFunction::CumeDist
            } else if let Some(ntile_expr) = built_in_fn_expr.downcast_ref::<Ntile>() {
                args.insert(
                    0,
                    Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                        ntile_expr.get_n() as i64,
                    )))),
                );
                protobuf::BuiltInWindowFunction::Ntile
            } else if let Some(window_shift_expr) =
                built_in_fn_expr.downcast_ref::<WindowShift>()
            {
                args.insert(
                    1,
                    Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                        window_shift_expr.get_shift_offset(),
                    )))),
                );
                args.insert(
                    2,
                    Arc::new(Literal::new(window_shift_expr.get_default_value())),
                );

                if window_shift_expr.get_shift_offset() >= 0 {
                    protobuf::BuiltInWindowFunction::Lag
                } else {
                    protobuf::BuiltInWindowFunction::Lead
                }
            } else if let Some(nth_value_expr) =
                built_in_fn_expr.downcast_ref::<NthValue>()
            {
                match nth_value_expr.get_kind() {
                    NthValueKind::First => protobuf::BuiltInWindowFunction::FirstValue,
                    NthValueKind::Last => protobuf::BuiltInWindowFunction::LastValue,
                    NthValueKind::Nth(n) => {
                        args.insert(
                            1,
                            Arc::new(Literal::new(
                                datafusion_common::ScalarValue::Int64(Some(n)),
                            )),
                        );
                        protobuf::BuiltInWindowFunction::NthValue
                    }
                }
            } else {
                return not_impl_err!("BuiltIn function not supported: {expr:?}");
            };

            physical_window_expr_node::WindowFunction::BuiltInFunction(builtin_fn as i32)
        } else if let Some(plain_aggr_window_expr) =
            expr.downcast_ref::<PlainAggregateWindowExpr>()
        {
            let AggrFn { inner, distinct } = aggr_expr_to_aggr_fn(
                plain_aggr_window_expr.get_aggregate_expr().as_ref(),
            )?;

            if distinct {
                // TODO
                return not_impl_err!(
                    "Distinct aggregate functions not supported in window expressions"
                );
            }

            if !window_frame.start_bound.is_unbounded() {
                return Err(DataFusionError::Internal(format!("Invalid PlainAggregateWindowExpr = {window_expr:?} with WindowFrame = {window_frame:?}")));
            }

            physical_window_expr_node::WindowFunction::AggrFunction(inner as i32)
        } else if let Some(sliding_aggr_window_expr) =
            expr.downcast_ref::<SlidingAggregateWindowExpr>()
        {
            let AggrFn { inner, distinct } = aggr_expr_to_aggr_fn(
                sliding_aggr_window_expr.get_aggregate_expr().as_ref(),
            )?;

            if distinct {
                // TODO
                return not_impl_err!(
                    "Distinct aggregate functions not supported in window expressions"
                );
            }

            if window_frame.start_bound.is_unbounded() {
                return Err(DataFusionError::Internal(format!("Invalid SlidingAggregateWindowExpr = {window_expr:?} with WindowFrame = {window_frame:?}")));
            }

            physical_window_expr_node::WindowFunction::AggrFunction(inner as i32)
        } else {
            return not_impl_err!("WindowExpr not supported: {window_expr:?}");
        };
        let codec = DefaultPhysicalExtensionCodec {};
        let args = serialize_physical_exprs(args, &codec)?;
        let partition_by =
            serialize_physical_exprs(window_expr.partition_by().to_vec(), &codec)?;

        let order_by =
            serialize_physical_sort_exprs(window_expr.order_by().to_vec(), &codec)?;

        let window_frame: protobuf::WindowFrame = window_frame
            .as_ref()
            .try_into()
            .map_err(|e| DataFusionError::Internal(format!("{e}")))?;

        let name = window_expr.name().to_string();

        Ok(protobuf::PhysicalWindowExprNode {
            args,
            partition_by,
            order_by,
            window_frame: Some(window_frame),
            window_function: Some(window_function),
            name,
        })
    }
}

struct AggrFn {
    inner: protobuf::AggregateFunction,
    distinct: bool,
}

fn aggr_expr_to_aggr_fn(expr: &dyn AggregateExpr) -> Result<AggrFn> {
    let aggr_expr = expr.as_any();
    let mut distinct = false;

    let inner = if aggr_expr.downcast_ref::<Count>().is_some() {
        protobuf::AggregateFunction::Count
    } else if aggr_expr.downcast_ref::<DistinctCount>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::Count
    } else if aggr_expr.downcast_ref::<Grouping>().is_some() {
        protobuf::AggregateFunction::Grouping
    } else if aggr_expr.downcast_ref::<BitAnd>().is_some() {
        protobuf::AggregateFunction::BitAnd
    } else if aggr_expr.downcast_ref::<BitOr>().is_some() {
        protobuf::AggregateFunction::BitOr
    } else if aggr_expr.downcast_ref::<BitXor>().is_some() {
        protobuf::AggregateFunction::BitXor
    } else if aggr_expr.downcast_ref::<DistinctBitXor>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::BitXor
    } else if aggr_expr.downcast_ref::<BoolAnd>().is_some() {
        protobuf::AggregateFunction::BoolAnd
    } else if aggr_expr.downcast_ref::<BoolOr>().is_some() {
        protobuf::AggregateFunction::BoolOr
    } else if aggr_expr.downcast_ref::<Sum>().is_some() {
        protobuf::AggregateFunction::Sum
    } else if aggr_expr.downcast_ref::<DistinctSum>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::Sum
    } else if aggr_expr.downcast_ref::<ApproxDistinct>().is_some() {
        protobuf::AggregateFunction::ApproxDistinct
    } else if aggr_expr.downcast_ref::<ArrayAgg>().is_some() {
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<DistinctArrayAgg>().is_some() {
        distinct = true;
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<OrderSensitiveArrayAgg>().is_some() {
        protobuf::AggregateFunction::ArrayAgg
    } else if aggr_expr.downcast_ref::<Min>().is_some() {
        protobuf::AggregateFunction::Min
    } else if aggr_expr.downcast_ref::<Max>().is_some() {
        protobuf::AggregateFunction::Max
    } else if aggr_expr.downcast_ref::<Avg>().is_some() {
        protobuf::AggregateFunction::Avg
    } else if aggr_expr.downcast_ref::<Variance>().is_some() {
        protobuf::AggregateFunction::Variance
    } else if aggr_expr.downcast_ref::<VariancePop>().is_some() {
        protobuf::AggregateFunction::VariancePop
    } else if aggr_expr.downcast_ref::<Covariance>().is_some() {
        protobuf::AggregateFunction::Covariance
    } else if aggr_expr.downcast_ref::<CovariancePop>().is_some() {
        protobuf::AggregateFunction::CovariancePop
    } else if aggr_expr.downcast_ref::<Stddev>().is_some() {
        protobuf::AggregateFunction::Stddev
    } else if aggr_expr.downcast_ref::<StddevPop>().is_some() {
        protobuf::AggregateFunction::StddevPop
    } else if aggr_expr.downcast_ref::<Correlation>().is_some() {
        protobuf::AggregateFunction::Correlation
    } else if let Some(regr_expr) = aggr_expr.downcast_ref::<Regr>() {
        match regr_expr.get_regr_type() {
            RegrType::Slope => protobuf::AggregateFunction::RegrSlope,
            RegrType::Intercept => protobuf::AggregateFunction::RegrIntercept,
            RegrType::Count => protobuf::AggregateFunction::RegrCount,
            RegrType::R2 => protobuf::AggregateFunction::RegrR2,
            RegrType::AvgX => protobuf::AggregateFunction::RegrAvgx,
            RegrType::AvgY => protobuf::AggregateFunction::RegrAvgy,
            RegrType::SXX => protobuf::AggregateFunction::RegrSxx,
            RegrType::SYY => protobuf::AggregateFunction::RegrSyy,
            RegrType::SXY => protobuf::AggregateFunction::RegrSxy,
        }
    } else if aggr_expr.downcast_ref::<ApproxPercentileCont>().is_some() {
        protobuf::AggregateFunction::ApproxPercentileCont
    } else if aggr_expr
        .downcast_ref::<ApproxPercentileContWithWeight>()
        .is_some()
    {
        protobuf::AggregateFunction::ApproxPercentileContWithWeight
    } else if aggr_expr.downcast_ref::<ApproxMedian>().is_some() {
        protobuf::AggregateFunction::ApproxMedian
    } else if aggr_expr.downcast_ref::<Median>().is_some() {
        protobuf::AggregateFunction::Median
    } else if aggr_expr.downcast_ref::<FirstValue>().is_some() {
        protobuf::AggregateFunction::FirstValueAgg
    } else if aggr_expr.downcast_ref::<LastValue>().is_some() {
        protobuf::AggregateFunction::LastValueAgg
    } else if aggr_expr.downcast_ref::<StringAgg>().is_some() {
        protobuf::AggregateFunction::StringAgg
    } else if aggr_expr.downcast_ref::<NthValueAgg>().is_some() {
        protobuf::AggregateFunction::NthValueAgg
    } else {
        return not_impl_err!("Aggregate function not supported: {expr:?}");
    };

    Ok(AggrFn { inner, distinct })
}

pub fn serialize_physical_sort_exprs<I>(
    sort_exprs: I,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<protobuf::PhysicalSortExprNode>, DataFusionError>
where
    I: IntoIterator<Item = PhysicalSortExpr>,
{
    sort_exprs
        .into_iter()
        .map(|sort_expr| serialize_physical_sort_expr(sort_expr, codec))
        .collect()
}

pub fn serialize_physical_sort_expr(
    sort_expr: PhysicalSortExpr,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalSortExprNode, DataFusionError> {
    let PhysicalSortExpr { expr, options } = sort_expr;
    let expr = serialize_physical_expr(expr, codec)?;
    Ok(PhysicalSortExprNode {
        expr: Some(Box::new(expr)),
        asc: !options.descending,
        nulls_first: options.nulls_first,
    })
}

pub fn serialize_physical_exprs<I>(
    values: I,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Vec<protobuf::PhysicalExprNode>, DataFusionError>
where
    I: IntoIterator<Item = Arc<dyn PhysicalExpr>>,
{
    values
        .into_iter()
        .map(|value| serialize_physical_expr(value, codec))
        .collect()
}

/// Serialize a `PhysicalExpr` to default protobuf representation.
///
/// If required, a [`PhysicalExtensionCodec`] can be provided which can handle
/// serialization of udfs requiring specialized serialization (see [`PhysicalExtensionCodec::try_encode_udf`])
pub fn serialize_physical_expr(
    value: Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalExprNode, DataFusionError> {
    let expr = value.as_any();

    if let Some(expr) = expr.downcast_ref::<Column>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Column(
                protobuf::PhysicalColumn {
                    name: expr.name().to_string(),
                    index: expr.index() as u32,
                },
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<BinaryExpr>() {
        let binary_expr = Box::new(protobuf::PhysicalBinaryExprNode {
            l: Some(Box::new(serialize_physical_expr(
                expr.left().clone(),
                codec,
            )?)),
            r: Some(Box::new(serialize_physical_expr(
                expr.right().clone(),
                codec,
            )?)),
            op: format!("{:?}", expr.op()),
        });

        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::BinaryExpr(
                binary_expr,
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<CaseExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(
                protobuf::physical_expr_node::ExprType::Case(
                    Box::new(
                        protobuf::PhysicalCaseNode {
                            expr: expr
                                .expr()
                                .map(|exp| {
                                    serialize_physical_expr(exp.clone(), codec)
                                        .map(Box::new)
                                })
                                .transpose()?,
                            when_then_expr: expr
                                .when_then_expr()
                                .iter()
                                .map(|(when_expr, then_expr)| {
                                    try_parse_when_then_expr(when_expr, then_expr, codec)
                                })
                                .collect::<Result<
                                    Vec<protobuf::PhysicalWhenThen>,
                                    DataFusionError,
                                >>()?,
                            else_expr: expr
                                .else_expr()
                                .map(|a| {
                                    serialize_physical_expr(a.clone(), codec)
                                        .map(Box::new)
                                })
                                .transpose()?,
                        },
                    ),
                ),
            ),
        })
    } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(Box::new(
                protobuf::PhysicalNot {
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.arg().to_owned(),
                        codec,
                    )?)),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                Box::new(protobuf::PhysicalIsNull {
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.arg().to_owned(),
                        codec,
                    )?)),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                Box::new(protobuf::PhysicalIsNotNull {
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.arg().to_owned(),
                        codec,
                    )?)),
                }),
            )),
        })
    } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::InList(Box::new(
                protobuf::PhysicalInListNode {
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.expr().to_owned(),
                        codec,
                    )?)),
                    list: serialize_physical_exprs(expr.list().to_vec(), codec)?,
                    negated: expr.negated(),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(Box::new(
                protobuf::PhysicalNegativeNode {
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.arg().to_owned(),
                        codec,
                    )?)),
                },
            ))),
        })
    } else if let Some(lit) = expr.downcast_ref::<Literal>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Literal(
                lit.value().try_into()?,
            )),
        })
    } else if let Some(cast) = expr.downcast_ref::<CastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::Cast(Box::new(
                protobuf::PhysicalCastNode {
                    expr: Some(Box::new(serialize_physical_expr(
                        cast.expr().to_owned(),
                        codec,
                    )?)),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(Box::new(
                protobuf::PhysicalTryCastNode {
                    expr: Some(Box::new(serialize_physical_expr(
                        cast.expr().to_owned(),
                        codec,
                    )?)),
                    arrow_type: Some(cast.cast_type().try_into()?),
                },
            ))),
        })
    } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
        let args = serialize_physical_exprs(expr.args().to_vec(), codec)?;
        if let Ok(fun) = BuiltinScalarFunction::from_str(expr.name()) {
            let fun: protobuf::ScalarFunction = (&fun).try_into()?;

            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarFunction(
                    protobuf::PhysicalScalarFunctionNode {
                        name: expr.name().to_string(),
                        fun: fun.into(),
                        args,
                        return_type: Some(expr.return_type().try_into()?),
                    },
                )),
            })
        } else {
            let mut buf = Vec::new();
            match expr.fun() {
                ScalarFunctionDefinition::UDF(udf) => {
                    codec.try_encode_udf(udf, &mut buf)?;
                }
                _ => {
                    return not_impl_err!(
                        "Proto serialization error: Trying to serialize a unresolved function"
                    );
                }
            }

            let fun_definition = if buf.is_empty() { None } else { Some(buf) };
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                    protobuf::PhysicalScalarUdfNode {
                        name: expr.name().to_string(),
                        args,
                        fun_definition,
                        return_type: Some(expr.return_type().try_into()?),
                    },
                )),
            })
        }
    } else if let Some(expr) = expr.downcast_ref::<LikeExpr>() {
        Ok(protobuf::PhysicalExprNode {
            expr_type: Some(protobuf::physical_expr_node::ExprType::LikeExpr(Box::new(
                protobuf::PhysicalLikeExprNode {
                    negated: expr.negated(),
                    case_insensitive: expr.case_insensitive(),
                    expr: Some(Box::new(serialize_physical_expr(
                        expr.expr().to_owned(),
                        codec,
                    )?)),
                    pattern: Some(Box::new(serialize_physical_expr(
                        expr.pattern().to_owned(),
                        codec,
                    )?)),
                },
            ))),
        })
    } else {
        internal_err!("physical_plan::to_proto() unsupported expression {value:?}")
    }
}

fn try_parse_when_then_expr(
    when_expr: &Arc<dyn PhysicalExpr>,
    then_expr: &Arc<dyn PhysicalExpr>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<protobuf::PhysicalWhenThen> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(serialize_physical_expr(when_expr.clone(), codec)?),
        then_expr: Some(serialize_physical_expr(then_expr.clone(), codec)?),
    })
}

impl TryFrom<&PartitionedFile> for protobuf::PartitionedFile {
    type Error = DataFusionError;

    fn try_from(pf: &PartitionedFile) -> Result<Self, Self::Error> {
        let last_modified = pf.object_meta.last_modified;
        let last_modified_ns = last_modified.timestamp_nanos_opt().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Invalid timestamp on PartitionedFile::ObjectMeta: {last_modified}"
            ))
        })? as u64;
        Ok(protobuf::PartitionedFile {
            path: pf.object_meta.location.as_ref().to_owned(),
            size: pf.object_meta.size as u64,
            last_modified_ns,
            partition_values: pf
                .partition_values
                .iter()
                .map(|v| v.try_into())
                .collect::<Result<Vec<_>, _>>()?,
            range: pf.range.as_ref().map(|r| r.try_into()).transpose()?,
            statistics: pf.statistics.as_ref().map(|s| s.into()),
        })
    }
}

impl TryFrom<&FileRange> for protobuf::FileRange {
    type Error = DataFusionError;

    fn try_from(value: &FileRange) -> Result<Self, Self::Error> {
        Ok(protobuf::FileRange {
            start: value.start,
            end: value.end,
        })
    }
}

impl TryFrom<&[PartitionedFile]> for protobuf::FileGroup {
    type Error = DataFusionError;

    fn try_from(gr: &[PartitionedFile]) -> Result<Self, Self::Error> {
        Ok(protobuf::FileGroup {
            files: gr
                .iter()
                .map(|f| f.try_into())
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<&Precision<usize>> for protobuf::Precision {
    fn from(s: &Precision<usize>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: Some(ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: Some(ScalarValue {
                    value: Some(Value::Uint64Value(*val as u64)),
                }),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(ScalarValue { value: None }),
            },
        }
    }
}

impl From<&Precision<datafusion_common::ScalarValue>> for protobuf::Precision {
    fn from(s: &Precision<datafusion_common::ScalarValue>) -> protobuf::Precision {
        match s {
            Precision::Exact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Exact.into(),
                val: val.try_into().ok(),
            },
            Precision::Inexact(val) => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Inexact.into(),
                val: val.try_into().ok(),
            },
            Precision::Absent => protobuf::Precision {
                precision_info: protobuf::PrecisionInfo::Absent.into(),
                val: Some(ScalarValue { value: None }),
            },
        }
    }
}

impl From<&Statistics> for protobuf::Statistics {
    fn from(s: &Statistics) -> protobuf::Statistics {
        let column_stats = s.column_statistics.iter().map(|s| s.into()).collect();
        protobuf::Statistics {
            num_rows: Some(protobuf::Precision::from(&s.num_rows)),
            total_byte_size: Some(protobuf::Precision::from(&s.total_byte_size)),
            column_stats,
        }
    }
}

impl From<&ColumnStatistics> for protobuf::ColumnStats {
    fn from(s: &ColumnStatistics) -> protobuf::ColumnStats {
        protobuf::ColumnStats {
            min_value: Some(protobuf::Precision::from(&s.min_value)),
            max_value: Some(protobuf::Precision::from(&s.max_value)),
            null_count: Some(protobuf::Precision::from(&s.null_count)),
            distinct_count: Some(protobuf::Precision::from(&s.distinct_count)),
        }
    }
}

impl TryFrom<&FileScanConfig> for protobuf::FileScanExecConf {
    type Error = DataFusionError;
    fn try_from(
        conf: &FileScanConfig,
    ) -> Result<protobuf::FileScanExecConf, Self::Error> {
        let codec = DefaultPhysicalExtensionCodec {};
        let file_groups = conf
            .file_groups
            .iter()
            .map(|p| p.as_slice().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        let mut output_orderings = vec![];
        for order in &conf.output_ordering {
            let ordering = serialize_physical_sort_exprs(order.to_vec(), &codec)?;
            output_orderings.push(ordering)
        }

        // Fields must be added to the schema so that they can persist in the protobuf
        // and then they are to be removed from the schema in `parse_protobuf_file_scan_config`
        let mut fields = conf
            .file_schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        fields.extend(conf.table_partition_cols.iter().cloned().map(Arc::new));
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields.clone()));

        Ok(protobuf::FileScanExecConf {
            file_groups,
            statistics: Some((&conf.statistics).into()),
            limit: conf.limit.map(|l| protobuf::ScanLimit { limit: l as u32 }),
            projection: conf
                .projection
                .as_ref()
                .unwrap_or(&vec![])
                .iter()
                .map(|n| *n as u32)
                .collect(),
            schema: Some(schema.as_ref().try_into()?),
            table_partition_cols: conf
                .table_partition_cols
                .iter()
                .map(|x| x.name().clone())
                .collect::<Vec<_>>(),
            object_store_url: conf.object_store_url.to_string(),
            output_ordering: output_orderings
                .into_iter()
                .map(|e| PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes: e,
                })
                .collect::<Vec<_>>(),
        })
    }
}

impl From<JoinSide> for protobuf::JoinSide {
    fn from(t: JoinSide) -> Self {
        match t {
            JoinSide::Left => protobuf::JoinSide::LeftSide,
            JoinSide::Right => protobuf::JoinSide::RightSide,
        }
    }
}

impl TryFrom<Option<Arc<dyn PhysicalExpr>>> for protobuf::MaybeFilter {
    type Error = DataFusionError;

    fn try_from(expr: Option<Arc<dyn PhysicalExpr>>) -> Result<Self, Self::Error> {
        let codec = DefaultPhysicalExtensionCodec {};
        match expr {
            None => Ok(protobuf::MaybeFilter { expr: None }),
            Some(expr) => Ok(protobuf::MaybeFilter {
                expr: Some(serialize_physical_expr(expr, &codec)?),
            }),
        }
    }
}

impl TryFrom<Option<Vec<PhysicalSortExpr>>> for protobuf::MaybePhysicalSortExprs {
    type Error = DataFusionError;

    fn try_from(sort_exprs: Option<Vec<PhysicalSortExpr>>) -> Result<Self, Self::Error> {
        match sort_exprs {
            None => Ok(protobuf::MaybePhysicalSortExprs { sort_expr: vec![] }),
            Some(sort_exprs) => Ok(protobuf::MaybePhysicalSortExprs {
                sort_expr: sort_exprs
                    .into_iter()
                    .map(|sort_expr| sort_expr.try_into())
                    .collect::<Result<Vec<_>>>()?,
            }),
        }
    }
}

impl TryFrom<PhysicalSortExpr> for protobuf::PhysicalSortExprNode {
    type Error = DataFusionError;

    fn try_from(sort_expr: PhysicalSortExpr) -> std::result::Result<Self, Self::Error> {
        let codec = DefaultPhysicalExtensionCodec {};
        Ok(PhysicalSortExprNode {
            expr: Some(Box::new(serialize_physical_expr(sort_expr.expr, &codec)?)),
            asc: !sort_expr.options.descending,
            nulls_first: sort_expr.options.nulls_first,
        })
    }
}

impl TryFrom<&JsonSink> for protobuf::JsonSink {
    type Error = DataFusionError;

    fn try_from(value: &JsonSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

impl TryFrom<&CsvSink> for protobuf::CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &CsvSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            writer_options: Some(value.writer_options().try_into()?),
        })
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
            parquet_options: Some(value.parquet_options().try_into()?),
        })
    }
}

impl TryFrom<&FileSinkConfig> for protobuf::FileSinkConfig {
    type Error = DataFusionError;

    fn try_from(conf: &FileSinkConfig) -> Result<Self, Self::Error> {
        let file_groups = conf
            .file_groups
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let table_paths = conf
            .table_paths
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let table_partition_cols = conf
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| {
                Ok(protobuf::PartitionColumn {
                    name: name.to_owned(),
                    arrow_type: Some(data_type.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            object_store_url: conf.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(conf.output_schema.as_ref().try_into()?),
            table_partition_cols,
            overwrite: conf.overwrite,
        })
    }
}

impl From<&CompressionTypeVariant> for protobuf::CompressionTypeVariant {
    fn from(value: &CompressionTypeVariant) -> Self {
        match value {
            CompressionTypeVariant::GZIP => Self::Gzip,
            CompressionTypeVariant::BZIP2 => Self::Bzip2,
            CompressionTypeVariant::XZ => Self::Xz,
            CompressionTypeVariant::ZSTD => Self::Zstd,
            CompressionTypeVariant::UNCOMPRESSED => Self::Uncompressed,
        }
    }
}

impl TryFrom<&CsvWriterOptions> for protobuf::CsvWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &CsvWriterOptions) -> Result<Self, Self::Error> {
        Ok(csv_writer_options_to_proto(
            &opts.writer_options,
            &opts.compression,
        ))
    }
}

impl TryFrom<&JsonWriterOptions> for protobuf::JsonWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &JsonWriterOptions) -> Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonWriterOptions {
            compression: compression.into(),
        })
    }
}

impl TryFrom<&ParquetOptions> for protobuf::ParquetOptions {
    type Error = DataFusionError;

    fn try_from(value: &ParquetOptions) -> Result<Self, Self::Error> {
        Ok(protobuf::ParquetOptions {
            enable_page_index: value.enable_page_index,
            pruning: value.pruning,
            skip_metadata: value.skip_metadata,
            metadata_size_hint_opt: value.metadata_size_hint.map(|v| protobuf::parquet_options::MetadataSizeHintOpt::MetadataSizeHint(v as u64)),
            pushdown_filters: value.pushdown_filters,
            reorder_filters: value.reorder_filters,
            data_pagesize_limit: value.data_pagesize_limit as u64,
            write_batch_size: value.write_batch_size as u64,
            writer_version: value.writer_version.clone(),
            compression_opt: value.compression.clone().map(protobuf::parquet_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value.dictionary_enabled.map(protobuf::parquet_options::DictionaryEnabledOpt::DictionaryEnabled),
            dictionary_page_size_limit: value.dictionary_page_size_limit as u64,
            statistics_enabled_opt: value.statistics_enabled.clone().map(protobuf::parquet_options::StatisticsEnabledOpt::StatisticsEnabled),
            max_statistics_size_opt: value.max_statistics_size.map(|v| protobuf::parquet_options::MaxStatisticsSizeOpt::MaxStatisticsSize(v as u64)),
            max_row_group_size: value.max_row_group_size as u64,
            created_by: value.created_by.clone(),
            column_index_truncate_length_opt: value.column_index_truncate_length.map(|v| protobuf::parquet_options::ColumnIndexTruncateLengthOpt::ColumnIndexTruncateLength(v as u64)),
            data_page_row_count_limit: value.data_page_row_count_limit as u64,
            encoding_opt: value.encoding.clone().map(protobuf::parquet_options::EncodingOpt::Encoding),
            bloom_filter_enabled: value.bloom_filter_enabled,
            bloom_filter_fpp_opt: value.bloom_filter_fpp.map(protobuf::parquet_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value.bloom_filter_ndv.map(protobuf::parquet_options::BloomFilterNdvOpt::BloomFilterNdv),
            allow_single_file_parallelism: value.allow_single_file_parallelism,
            maximum_parallel_row_group_writers: value.maximum_parallel_row_group_writers as u64,
            maximum_buffered_record_batches_per_stream: value.maximum_buffered_record_batches_per_stream as u64,
        })
    }
}

impl TryFrom<&ColumnOptions> for protobuf::ColumnOptions {
    type Error = DataFusionError;

    fn try_from(value: &ColumnOptions) -> Result<Self, Self::Error> {
        Ok(protobuf::ColumnOptions {
            compression_opt: value
                .compression
                .clone()
                .map(protobuf::column_options::CompressionOpt::Compression),
            dictionary_enabled_opt: value
                .dictionary_enabled
                .map(protobuf::column_options::DictionaryEnabledOpt::DictionaryEnabled),
            statistics_enabled_opt: value
                .statistics_enabled
                .clone()
                .map(protobuf::column_options::StatisticsEnabledOpt::StatisticsEnabled),
            max_statistics_size_opt: value.max_statistics_size.map(|v| {
                protobuf::column_options::MaxStatisticsSizeOpt::MaxStatisticsSize(
                    v as u32,
                )
            }),
            encoding_opt: value
                .encoding
                .clone()
                .map(protobuf::column_options::EncodingOpt::Encoding),
            bloom_filter_enabled_opt: value
                .bloom_filter_enabled
                .map(protobuf::column_options::BloomFilterEnabledOpt::BloomFilterEnabled),
            bloom_filter_fpp_opt: value
                .bloom_filter_fpp
                .map(protobuf::column_options::BloomFilterFppOpt::BloomFilterFpp),
            bloom_filter_ndv_opt: value
                .bloom_filter_ndv
                .map(protobuf::column_options::BloomFilterNdvOpt::BloomFilterNdv),
        })
    }
}

impl TryFrom<&TableParquetOptions> for protobuf::TableParquetOptions {
    type Error = DataFusionError;
    fn try_from(value: &TableParquetOptions) -> Result<Self, Self::Error> {
        let column_specific_options = value
            .column_specific_options
            .iter()
            .map(|(k, v)| {
                Ok(protobuf::ColumnSpecificOptions {
                    column_name: k.into(),
                    options: Some(v.try_into()?),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(protobuf::TableParquetOptions {
            global: Some((&value.global).try_into()?),
            column_specific_options,
        })
    }
}

impl TryFrom<&CsvOptions> for protobuf::CsvOptions {
    type Error = DataFusionError; // Define or use an appropriate error type

    fn try_from(opts: &CsvOptions) -> Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::CsvOptions {
            has_header: opts.has_header,
            delimiter: vec![opts.delimiter],
            quote: vec![opts.quote],
            escape: opts.escape.map_or_else(Vec::new, |e| vec![e]),
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec as u64,
            date_format: opts.date_format.clone().unwrap_or_default(),
            datetime_format: opts.datetime_format.clone().unwrap_or_default(),
            timestamp_format: opts.timestamp_format.clone().unwrap_or_default(),
            timestamp_tz_format: opts.timestamp_tz_format.clone().unwrap_or_default(),
            time_format: opts.time_format.clone().unwrap_or_default(),
            null_value: opts.null_value.clone().unwrap_or_default(),
        })
    }
}

impl TryFrom<&JsonOptions> for protobuf::JsonOptions {
    type Error = DataFusionError;

    fn try_from(opts: &JsonOptions) -> Result<Self, Self::Error> {
        let compression: protobuf::CompressionTypeVariant = opts.compression.into();
        Ok(protobuf::JsonOptions {
            compression: compression.into(),
            schema_infer_max_rec: opts.schema_infer_max_rec as u64,
        })
    }
}

impl TryFrom<&FormatOptions> for copy_to_node::FormatOptions {
    type Error = DataFusionError;
    fn try_from(value: &FormatOptions) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            FormatOptions::CSV(options) => {
                copy_to_node::FormatOptions::Csv(options.try_into()?)
            }
            FormatOptions::JSON(options) => {
                copy_to_node::FormatOptions::Json(options.try_into()?)
            }
            FormatOptions::PARQUET(options) => {
                copy_to_node::FormatOptions::Parquet(options.try_into()?)
            }
            FormatOptions::AVRO => copy_to_node::FormatOptions::Avro(AvroOptions {}),
            FormatOptions::ARROW => copy_to_node::FormatOptions::Arrow(ArrowOptions {}),
        })
    }
}
