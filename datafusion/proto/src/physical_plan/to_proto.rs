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

use crate::protobuf::{self, physical_window_expr_node, scalar_value::Value};
use crate::protobuf::{
    physical_aggregate_expr_node, PhysicalSortExprNode, PhysicalSortExprNodeCollection,
    ScalarValue,
};

#[cfg(feature = "parquet")]
use datafusion::datasource::file_format::parquet::ParquetSink;

use crate::logical_plan::{csv_writer_options_to_proto, writer_properties_to_proto};
use datafusion::datasource::{
    file_format::csv::CsvSink,
    file_format::json::JsonSink,
    listing::{FileRange, PartitionedFile},
    physical_plan::FileScanConfig,
    physical_plan::FileSinkConfig,
};
use datafusion::logical_expr::BuiltinScalarFunction;
use datafusion::physical_expr::expressions::{GetFieldAccessExpr, GetIndexedFieldExpr};
use datafusion::physical_expr::window::{NthValueKind, SlidingAggregateWindowExpr};
use datafusion::physical_expr::{PhysicalSortExpr, ScalarFunctionExpr};
use datafusion::physical_plan::expressions::{
    ApproxDistinct, ApproxMedian, ApproxPercentileCont, ApproxPercentileContWithWeight,
    ArrayAgg, Avg, BinaryExpr, BitAnd, BitOr, BitXor, BoolAnd, BoolOr, CaseExpr,
    CastExpr, Column, Correlation, Count, Covariance, CovariancePop, CumeDist,
    DistinctArrayAgg, DistinctBitXor, DistinctCount, DistinctSum, FirstValue, Grouping,
    InListExpr, IsNotNullExpr, IsNullExpr, LastValue, LikeExpr, Literal, Max, Median,
    Min, NegativeExpr, NotExpr, NthValue, Ntile, OrderSensitiveArrayAgg, Rank, RankType,
    Regr, RegrType, RowNumber, Stddev, StddevPop, Sum, TryCastExpr, Variance,
    VariancePop, WindowShift,
};
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::windows::{BuiltInWindowExpr, PlainAggregateWindowExpr};
use datafusion::physical_plan::{
    AggregateExpr, ColumnStatistics, PhysicalExpr, Statistics, WindowExpr,
};
use datafusion_common::{
    file_options::{
        arrow_writer::ArrowWriterOptions, avro_writer::AvroWriterOptions,
        csv_writer::CsvWriterOptions, json_writer::JsonWriterOptions,
        parquet_writer::ParquetWriterOptions,
    },
    internal_err, not_impl_err,
    parsers::CompressionTypeVariant,
    stats::Precision,
    DataFusionError, FileTypeWriterOptions, JoinSide, Result,
};

impl TryFrom<Arc<dyn AggregateExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(a: Arc<dyn AggregateExpr>) -> Result<Self, Self::Error> {
        let expressions: Vec<protobuf::PhysicalExprNode> = a
            .expressions()
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>>>()?;

        let ordering_req: Vec<protobuf::PhysicalSortExprNode> = a
            .order_bys()
            .unwrap_or(&[])
            .iter()
            .map(|e| e.clone().try_into())
            .collect::<Result<Vec<_>>>()?;

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
                if let Some(default_value) = window_shift_expr.get_default_value() {
                    args.insert(2, Arc::new(Literal::new(default_value)));
                }
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

        let args = args
            .into_iter()
            .map(|e| e.try_into())
            .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

        let partition_by = window_expr
            .partition_by()
            .iter()
            .map(|p| p.clone().try_into())
            .collect::<Result<Vec<protobuf::PhysicalExprNode>>>()?;

        let order_by = window_expr
            .order_by()
            .iter()
            .map(|o| o.clone().try_into())
            .collect::<Result<Vec<protobuf::PhysicalSortExprNode>>>()?;

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
    } else {
        return not_impl_err!("Aggregate function not supported: {expr:?}");
    };

    Ok(AggrFn { inner, distinct })
}

impl TryFrom<Arc<dyn PhysicalExpr>> for protobuf::PhysicalExprNode {
    type Error = DataFusionError;

    fn try_from(value: Arc<dyn PhysicalExpr>) -> Result<Self, Self::Error> {
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
                l: Some(Box::new(expr.left().to_owned().try_into()?)),
                r: Some(Box::new(expr.right().to_owned().try_into()?)),
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
                                    .map(|exp| exp.clone().try_into().map(Box::new))
                                    .transpose()?,
                                when_then_expr: expr
                                    .when_then_expr()
                                    .iter()
                                    .map(|(when_expr, then_expr)| {
                                        try_parse_when_then_expr(when_expr, then_expr)
                                    })
                                    .collect::<Result<
                                        Vec<protobuf::PhysicalWhenThen>,
                                        Self::Error,
                                    >>()?,
                                else_expr: expr
                                    .else_expr()
                                    .map(|a| a.clone().try_into().map(Box::new))
                                    .transpose()?,
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NotExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::NotExpr(
                    Box::new(protobuf::PhysicalNot {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNullExpr(
                    Box::new(protobuf::PhysicalIsNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<IsNotNullExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::IsNotNullExpr(
                    Box::new(protobuf::PhysicalIsNotNull {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<InListExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::InList(
                        Box::new(
                            protobuf::PhysicalInListNode {
                                expr: Some(Box::new(expr.expr().to_owned().try_into()?)),
                                list: expr
                                    .list()
                                    .iter()
                                    .map(|a| a.clone().try_into())
                                    .collect::<Result<
                                    Vec<protobuf::PhysicalExprNode>,
                                    Self::Error,
                                >>()?,
                                negated: expr.negated(),
                            },
                        ),
                    ),
                ),
            })
        } else if let Some(expr) = expr.downcast_ref::<NegativeExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::Negative(
                    Box::new(protobuf::PhysicalNegativeNode {
                        expr: Some(Box::new(expr.arg().to_owned().try_into()?)),
                    }),
                )),
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
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().try_into()?),
                    },
                ))),
            })
        } else if let Some(cast) = expr.downcast_ref::<TryCastExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::TryCast(
                    Box::new(protobuf::PhysicalTryCastNode {
                        expr: Some(Box::new(cast.expr().clone().try_into()?)),
                        arrow_type: Some(cast.cast_type().try_into()?),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<ScalarFunctionExpr>() {
            let args: Vec<protobuf::PhysicalExprNode> = expr
                .args()
                .iter()
                .map(|e| e.to_owned().try_into())
                .collect::<Result<Vec<_>, _>>()?;
            if let Ok(fun) = BuiltinScalarFunction::from_str(expr.name()) {
                let fun: protobuf::ScalarFunction = (&fun).try_into()?;

                Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(
                        protobuf::physical_expr_node::ExprType::ScalarFunction(
                            protobuf::PhysicalScalarFunctionNode {
                                name: expr.name().to_string(),
                                fun: fun.into(),
                                args,
                                return_type: Some(expr.return_type().try_into()?),
                            },
                        ),
                    ),
                })
            } else {
                Ok(protobuf::PhysicalExprNode {
                    expr_type: Some(protobuf::physical_expr_node::ExprType::ScalarUdf(
                        protobuf::PhysicalScalarUdfNode {
                            name: expr.name().to_string(),
                            args,
                            return_type: Some(expr.return_type().try_into()?),
                        },
                    )),
                })
            }
        } else if let Some(expr) = expr.downcast_ref::<LikeExpr>() {
            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(protobuf::physical_expr_node::ExprType::LikeExpr(
                    Box::new(protobuf::PhysicalLikeExprNode {
                        negated: expr.negated(),
                        case_insensitive: expr.case_insensitive(),
                        expr: Some(Box::new(expr.expr().to_owned().try_into()?)),
                        pattern: Some(Box::new(expr.pattern().to_owned().try_into()?)),
                    }),
                )),
            })
        } else if let Some(expr) = expr.downcast_ref::<GetIndexedFieldExpr>() {
            let field = match expr.field() {
                GetFieldAccessExpr::NamedStructField{name} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::NamedStructFieldExpr(protobuf::NamedStructFieldExpr {
                        name: Some(ScalarValue::try_from(name)?)
                    })
                ),
                GetFieldAccessExpr::ListIndex{key} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::ListIndexExpr(Box::new(protobuf::ListIndexExpr {
                        key: Some(Box::new(key.to_owned().try_into()?))
                    }))
                ),
                GetFieldAccessExpr::ListRange{start, stop} => Some(
                    protobuf::physical_get_indexed_field_expr_node::Field::ListRangeExpr(Box::new(protobuf::ListRangeExpr {
                        start: Some(Box::new(start.to_owned().try_into()?)),
                        stop: Some(Box::new(stop.to_owned().try_into()?)),
                    }))
                ),
            };

            Ok(protobuf::PhysicalExprNode {
                expr_type: Some(
                    protobuf::physical_expr_node::ExprType::GetIndexedFieldExpr(
                        Box::new(protobuf::PhysicalGetIndexedFieldExprNode {
                            arg: Some(Box::new(expr.arg().to_owned().try_into()?)),
                            field,
                        }),
                    ),
                ),
            })
        } else {
            internal_err!("physical_plan::to_proto() unsupported expression {value:?}")
        }
    }
}

fn try_parse_when_then_expr(
    when_expr: &Arc<dyn PhysicalExpr>,
    then_expr: &Arc<dyn PhysicalExpr>,
) -> Result<protobuf::PhysicalWhenThen> {
    Ok(protobuf::PhysicalWhenThen {
        when_expr: Some(when_expr.clone().try_into()?),
        then_expr: Some(then_expr.clone().try_into()?),
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
        let file_groups = conf
            .file_groups
            .iter()
            .map(|p| p.as_slice().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        let mut output_orderings = vec![];
        for order in &conf.output_ordering {
            let expr_node_vec = order
                .iter()
                .map(|sort_expr| {
                    let expr = sort_expr.expr.clone().try_into()?;
                    Ok(PhysicalSortExprNode {
                        expr: Some(Box::new(expr)),
                        asc: !sort_expr.options.descending,
                        nulls_first: sort_expr.options.nulls_first,
                    })
                })
                .collect::<Result<Vec<PhysicalSortExprNode>>>()?;
            output_orderings.push(expr_node_vec)
        }

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
            schema: Some(conf.file_schema.as_ref().try_into()?),
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
        match expr {
            None => Ok(protobuf::MaybeFilter { expr: None }),
            Some(expr) => Ok(protobuf::MaybeFilter {
                expr: Some(expr.try_into()?),
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
        Ok(PhysicalSortExprNode {
            expr: Some(Box::new(sort_expr.expr.try_into()?)),
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
        })
    }
}

impl TryFrom<&CsvSink> for protobuf::CsvSink {
    type Error = DataFusionError;

    fn try_from(value: &CsvSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
        })
    }
}

#[cfg(feature = "parquet")]
impl TryFrom<&ParquetSink> for protobuf::ParquetSink {
    type Error = DataFusionError;

    fn try_from(value: &ParquetSink) -> Result<Self, Self::Error> {
        Ok(Self {
            config: Some(value.config().try_into()?),
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
        let file_type_writer_options = &conf.file_type_writer_options;
        Ok(Self {
            object_store_url: conf.object_store_url.to_string(),
            file_groups,
            table_paths,
            output_schema: Some(conf.output_schema.as_ref().try_into()?),
            table_partition_cols,
            single_file_output: conf.single_file_output,
            overwrite: conf.overwrite,
            file_type_writer_options: Some(file_type_writer_options.try_into()?),
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

impl TryFrom<&FileTypeWriterOptions> for protobuf::FileTypeWriterOptions {
    type Error = DataFusionError;

    fn try_from(opts: &FileTypeWriterOptions) -> Result<Self, Self::Error> {
        let file_type = match opts {
            #[cfg(feature = "parquet")]
            FileTypeWriterOptions::Parquet(ParquetWriterOptions { writer_options }) => {
                protobuf::file_type_writer_options::FileType::ParquetOptions(
                    protobuf::ParquetWriterOptions {
                        writer_properties: Some(writer_properties_to_proto(
                            writer_options,
                        )),
                    },
                )
            }
            FileTypeWriterOptions::CSV(CsvWriterOptions {
                writer_options,
                compression,
            }) => protobuf::file_type_writer_options::FileType::CsvOptions(
                csv_writer_options_to_proto(writer_options, compression),
            ),
            FileTypeWriterOptions::JSON(JsonWriterOptions { compression }) => {
                let compression: protobuf::CompressionTypeVariant = compression.into();
                protobuf::file_type_writer_options::FileType::JsonOptions(
                    protobuf::JsonWriterOptions {
                        compression: compression.into(),
                    },
                )
            }
            FileTypeWriterOptions::Avro(AvroWriterOptions {}) => {
                return not_impl_err!("Avro file sink protobuf serialization")
            }
            FileTypeWriterOptions::Arrow(ArrowWriterOptions {}) => {
                return not_impl_err!("Arrow file sink protobuf serialization")
            }
            FileTypeWriterOptions::Extension(_) => {
                return not_impl_err!("Extension file sink protobuf serialization")
            }
        };
        Ok(Self {
            file_type: Some(file_type),
        })
    }
}
