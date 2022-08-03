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

//! Declaration of built-in (aggregate) functions.
//! This module contains built-in aggregates' enumeration and metadata.
//!
//! Generally, an aggregate has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for min, ([f32]) -> f32, ([f64]) -> f64.

use crate::aggregate::coercion_rule::coerce_exprs;
use crate::{expressions, AggregateExpr, PhysicalExpr};
use arrow::datatypes::Schema;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::aggregate_function;
use datafusion_expr::aggregate_function::return_type;
pub use datafusion_expr::AggregateFunction;
use std::sync::Arc;

/// Create a physical aggregation expression.
/// This function errors when `input_phy_exprs`' can't be coerced to a valid argument type of the aggregation function.
pub fn create_aggregate_expr(
    fun: &AggregateFunction,
    distinct: bool,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let name = name.into();
    // get the coerced phy exprs if some expr need to be wrapped with the try cast.
    let coerced_phy_exprs = coerce_exprs(
        fun,
        input_phy_exprs,
        input_schema,
        &aggregate_function::signature(fun),
    )?;
    if coerced_phy_exprs.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Invalid or wrong number of arguments passed to aggregate: '{}'",
            name,
        )));
    }
    let coerced_exprs_types = coerced_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;

    // get the result data type for this aggregate function
    let input_phy_types = input_phy_exprs
        .iter()
        .map(|e| e.data_type(input_schema))
        .collect::<Result<Vec<_>>>()?;
    let return_type = return_type(fun, &input_phy_types)?;

    Ok(match (fun, distinct) {
        (AggregateFunction::Count, false) => Arc::new(expressions::Count::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Count, true) => Arc::new(expressions::DistinctCount::new(
            coerced_exprs_types,
            coerced_phy_exprs,
            name,
            return_type,
        )),
        (AggregateFunction::Grouping, _) => Arc::new(expressions::Grouping::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Sum, false) => Arc::new(expressions::Sum::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Sum, true) => Arc::new(expressions::DistinctSum::new(
            vec![coerced_phy_exprs[0].clone()],
            name,
            return_type,
        )),
        (AggregateFunction::ApproxDistinct, _) => {
            Arc::new(expressions::ApproxDistinct::new(
                coerced_phy_exprs[0].clone(),
                name,
                coerced_exprs_types[0].clone(),
            ))
        }
        (AggregateFunction::ArrayAgg, false) => Arc::new(expressions::ArrayAgg::new(
            coerced_phy_exprs[0].clone(),
            name,
            coerced_exprs_types[0].clone(),
        )),
        (AggregateFunction::ArrayAgg, true) => {
            Arc::new(expressions::DistinctArrayAgg::new(
                coerced_phy_exprs[0].clone(),
                name,
                coerced_exprs_types[0].clone(),
            ))
        }
        (AggregateFunction::Min, _) => Arc::new(expressions::Min::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Max, _) => Arc::new(expressions::Max::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Avg, false) => Arc::new(expressions::Avg::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Avg, true) => {
            return Err(DataFusionError::NotImplemented(
                "AVG(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::Variance, false) => Arc::new(expressions::Variance::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Variance, true) => {
            return Err(DataFusionError::NotImplemented(
                "VAR(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::VariancePop, false) => {
            Arc::new(expressions::VariancePop::new(
                coerced_phy_exprs[0].clone(),
                name,
                return_type,
            ))
        }
        (AggregateFunction::VariancePop, true) => {
            return Err(DataFusionError::NotImplemented(
                "VAR_POP(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::Covariance, false) => Arc::new(expressions::Covariance::new(
            coerced_phy_exprs[0].clone(),
            coerced_phy_exprs[1].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Covariance, true) => {
            return Err(DataFusionError::NotImplemented(
                "COVAR(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::CovariancePop, false) => {
            Arc::new(expressions::CovariancePop::new(
                coerced_phy_exprs[0].clone(),
                coerced_phy_exprs[1].clone(),
                name,
                return_type,
            ))
        }
        (AggregateFunction::CovariancePop, true) => {
            return Err(DataFusionError::NotImplemented(
                "COVAR_POP(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::Stddev, false) => Arc::new(expressions::Stddev::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Stddev, true) => {
            return Err(DataFusionError::NotImplemented(
                "STDDEV(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::StddevPop, false) => Arc::new(expressions::StddevPop::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::StddevPop, true) => {
            return Err(DataFusionError::NotImplemented(
                "STDDEV_POP(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::Correlation, false) => {
            Arc::new(expressions::Correlation::new(
                coerced_phy_exprs[0].clone(),
                coerced_phy_exprs[1].clone(),
                name,
                return_type,
            ))
        }
        (AggregateFunction::Correlation, true) => {
            return Err(DataFusionError::NotImplemented(
                "CORR(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::ApproxPercentileCont, false) => {
            Arc::new(expressions::ApproxPercentileCont::new(
                // Pass in the desired percentile expr
                coerced_phy_exprs,
                name,
                return_type,
            )?)
        }
        (AggregateFunction::ApproxPercentileCont, true) => {
            return Err(DataFusionError::NotImplemented(
                "approx_percentile_cont(DISTINCT) aggregations are not available"
                    .to_string(),
            ));
        }
        (AggregateFunction::ApproxPercentileContWithWeight, false) => {
            Arc::new(expressions::ApproxPercentileContWithWeight::new(
                // Pass in the desired percentile expr
                coerced_phy_exprs,
                name,
                return_type,
            )?)
        }
        (AggregateFunction::ApproxPercentileContWithWeight, true) => {
            return Err(DataFusionError::NotImplemented(
                "approx_percentile_cont_with_weight(DISTINCT) aggregations are not available"
                    .to_string(),
            ));
        }
        (AggregateFunction::ApproxMedian, false) => {
            Arc::new(expressions::ApproxMedian::new(
                coerced_phy_exprs[0].clone(),
                name,
                return_type,
            ))
        }
        (AggregateFunction::ApproxMedian, true) => {
            return Err(DataFusionError::NotImplemented(
                "APPROX_MEDIAN(DISTINCT) aggregations are not available".to_string(),
            ));
        }
        (AggregateFunction::Median, false) => Arc::new(expressions::Median::new(
            coerced_phy_exprs[0].clone(),
            name,
            return_type,
        )),
        (AggregateFunction::Median, true) => {
            return Err(DataFusionError::NotImplemented(
                "MEDIAN(DISTINCT) aggregations are not available".to_string(),
            ));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{
        ApproxDistinct, ApproxMedian, ApproxPercentileCont, ArrayAgg, Avg, Correlation,
        Count, Covariance, DistinctArrayAgg, DistinctCount, Max, Min, Stddev, Sum,
        Variance,
    };
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::aggregate_function::NUMERICS;

    #[test]
    fn test_count_arragg_approx_expr() -> Result<()> {
        let funcs = vec![
            AggregateFunction::Count,
            AggregateFunction::ArrayAgg,
            AggregateFunction::ApproxDistinct,
        ];
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(10, 2),
            DataType::Utf8,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Count => {
                        assert!(result_agg_phy_exprs.as_any().is::<Count>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::Int64, true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ApproxDistinct => {
                        assert!(result_agg_phy_exprs.as_any().is::<ApproxDistinct>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, false),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::ArrayAgg => {
                        assert!(result_agg_phy_exprs.as_any().is::<ArrayAgg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new(
                                "c1",
                                DataType::List(Box::new(Field::new(
                                    "item",
                                    data_type.clone(),
                                    true
                                ))),
                                false
                            ),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };

                let result_distinct = create_aggregate_expr(
                    &fun,
                    true,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Count => {
                        assert!(result_distinct.as_any().is::<DistinctCount>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new("c1", DataType::Int64, true),
                            result_distinct.field().unwrap()
                        );
                    }
                    AggregateFunction::ApproxDistinct => {
                        assert!(result_distinct.as_any().is::<ApproxDistinct>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new("c1", DataType::UInt64, false),
                            result_distinct.field().unwrap()
                        );
                    }
                    AggregateFunction::ArrayAgg => {
                        assert!(result_distinct.as_any().is::<DistinctArrayAgg>());
                        assert_eq!("c1", result_distinct.name());
                        assert_eq!(
                            Field::new(
                                "c1",
                                DataType::List(Box::new(Field::new(
                                    "item",
                                    data_type.clone(),
                                    true
                                ))),
                                false
                            ),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_agg_approx_percentile_phy_expr() {
        for data_type in NUMERICS {
            let input_schema =
                Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
            let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                ),
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(0.2)))),
            ];
            let result_agg_phy_exprs = create_aggregate_expr(
                &AggregateFunction::ApproxPercentileCont,
                false,
                &input_phy_exprs[..],
                &input_schema,
                "c1",
            )
            .expect("failed to create aggregate expr");

            assert!(result_agg_phy_exprs.as_any().is::<ApproxPercentileCont>());
            assert_eq!("c1", result_agg_phy_exprs.name());
            assert_eq!(
                Field::new("c1", data_type.clone(), false),
                result_agg_phy_exprs.field().unwrap()
            );
        }
    }

    #[test]
    fn test_agg_approx_percentile_invalid_phy_expr() {
        for data_type in NUMERICS {
            let input_schema =
                Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
            let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                ),
                Arc::new(expressions::Literal::new(ScalarValue::Float64(Some(4.2)))),
            ];
            let err = create_aggregate_expr(
                &AggregateFunction::ApproxPercentileCont,
                false,
                &input_phy_exprs[..],
                &input_schema,
                "c1",
            )
            .expect_err("should fail due to invalid percentile");

            assert!(matches!(err, DataFusionError::Plan(_)));
        }
    }

    #[test]
    fn test_min_max_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Min, AggregateFunction::Max];
        let data_types = vec![
            DataType::UInt32,
            DataType::Int32,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(10, 2),
            DataType::Utf8,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Min => {
                        assert!(result_agg_phy_exprs.as_any().is::<Min>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Max => {
                        assert!(result_agg_phy_exprs.as_any().is::<Max>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", data_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_sum_avg_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Sum, AggregateFunction::Avg];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                match fun {
                    AggregateFunction::Sum => {
                        assert!(result_agg_phy_exprs.as_any().is::<Sum>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        let expect_type = match data_type {
                            DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64 => DataType::UInt64,
                            DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64 => DataType::Int64,
                            DataType::Float32 | DataType::Float64 => DataType::Float64,
                            _ => data_type.clone(),
                        };

                        assert_eq!(
                            Field::new("c1", expect_type.clone(), true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    AggregateFunction::Avg => {
                        assert!(result_agg_phy_exprs.as_any().is::<Avg>());
                        assert_eq!("c1", result_agg_phy_exprs.name());
                        assert_eq!(
                            Field::new("c1", DataType::Float64, true),
                            result_agg_phy_exprs.field().unwrap()
                        );
                    }
                    _ => {}
                };
            }
        }
        Ok(())
    }

    #[test]
    fn test_variance_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Variance];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Variance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_var_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::VariancePop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Variance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_stddev_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Stddev];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Stddev>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_stddev_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::StddevPop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Variance {
                    assert!(result_agg_phy_exprs.as_any().is::<Stddev>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_covar_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Covariance];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Covariance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_covar_pop_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::CovariancePop];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Covariance>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_corr_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::Correlation];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema = Schema::new(vec![
                    Field::new("c1", data_type.clone(), true),
                    Field::new("c2", data_type.clone(), true),
                ]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(
                        expressions::Column::new_with_schema("c1", &input_schema)
                            .unwrap(),
                    ),
                    Arc::new(
                        expressions::Column::new_with_schema("c2", &input_schema)
                            .unwrap(),
                    ),
                ];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..2],
                    &input_schema,
                    "c1",
                )?;
                if fun == AggregateFunction::Covariance {
                    assert!(result_agg_phy_exprs.as_any().is::<Correlation>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", DataType::Float64, true),
                        result_agg_phy_exprs.field().unwrap()
                    )
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_median_expr() -> Result<()> {
        let funcs = vec![AggregateFunction::ApproxMedian];
        let data_types = vec![
            DataType::UInt32,
            DataType::UInt64,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ];
        for fun in funcs {
            for data_type in &data_types {
                let input_schema =
                    Schema::new(vec![Field::new("c1", data_type.clone(), true)]);
                let input_phy_exprs: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(
                    expressions::Column::new_with_schema("c1", &input_schema).unwrap(),
                )];
                let result_agg_phy_exprs = create_aggregate_expr(
                    &fun,
                    false,
                    &input_phy_exprs[0..1],
                    &input_schema,
                    "c1",
                )?;

                if fun == AggregateFunction::ApproxMedian {
                    assert!(result_agg_phy_exprs.as_any().is::<ApproxMedian>());
                    assert_eq!("c1", result_agg_phy_exprs.name());
                    assert_eq!(
                        Field::new("c1", data_type.clone(), true),
                        result_agg_phy_exprs.field().unwrap()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_median() -> Result<()> {
        let observed = return_type(&AggregateFunction::ApproxMedian, &[DataType::Utf8]);
        assert!(observed.is_err());

        let observed = return_type(&AggregateFunction::ApproxMedian, &[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);

        let observed = return_type(
            &AggregateFunction::ApproxMedian,
            &[DataType::Decimal(10, 6)],
        );
        assert!(observed.is_err());

        Ok(())
    }

    #[test]
    fn test_min_max() -> Result<()> {
        let observed = return_type(&AggregateFunction::Min, &[DataType::Utf8])?;
        assert_eq!(DataType::Utf8, observed);

        let observed = return_type(&AggregateFunction::Max, &[DataType::Int32])?;
        assert_eq!(DataType::Int32, observed);

        // test decimal for min
        let observed = return_type(&AggregateFunction::Min, &[DataType::Decimal(10, 6)])?;
        assert_eq!(DataType::Decimal(10, 6), observed);

        // test decimal for max
        let observed =
            return_type(&AggregateFunction::Max, &[DataType::Decimal(28, 13)])?;
        assert_eq!(DataType::Decimal(28, 13), observed);

        Ok(())
    }

    #[test]
    fn test_sum_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::Int32])?;
        assert_eq!(DataType::Int64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::UInt8])?;
        assert_eq!(DataType::UInt64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Decimal(10, 5)])?;
        assert_eq!(DataType::Decimal(20, 5), observed);

        let observed = return_type(&AggregateFunction::Sum, &[DataType::Decimal(35, 5)])?;
        assert_eq!(DataType::Decimal(38, 5), observed);

        Ok(())
    }

    #[test]
    fn test_sum_no_utf8() {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_sum_upcasts() -> Result<()> {
        let observed = return_type(&AggregateFunction::Sum, &[DataType::UInt32])?;
        assert_eq!(DataType::UInt64, observed);
        Ok(())
    }

    #[test]
    fn test_count_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Count, &[DataType::Utf8])?;
        assert_eq!(DataType::Int64, observed);

        let observed = return_type(&AggregateFunction::Count, &[DataType::Int8])?;
        assert_eq!(DataType::Int64, observed);

        let observed =
            return_type(&AggregateFunction::Count, &[DataType::Decimal(28, 13)])?;
        assert_eq!(DataType::Int64, observed);
        Ok(())
    }

    #[test]
    fn test_avg_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Decimal(10, 6)])?;
        assert_eq!(DataType::Decimal(14, 10), observed);

        let observed = return_type(&AggregateFunction::Avg, &[DataType::Decimal(36, 6)])?;
        assert_eq!(DataType::Decimal(38, 10), observed);
        Ok(())
    }

    #[test]
    fn test_avg_no_utf8() {
        let observed = return_type(&AggregateFunction::Avg, &[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_variance_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Variance, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Variance, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Variance, &[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Variance, &[DataType::UInt32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Variance, &[DataType::Int64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_variance_no_utf8() {
        let observed = return_type(&AggregateFunction::Variance, &[DataType::Utf8]);
        assert!(observed.is_err());
    }

    #[test]
    fn test_stddev_return_type() -> Result<()> {
        let observed = return_type(&AggregateFunction::Stddev, &[DataType::Float32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Stddev, &[DataType::Float64])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Stddev, &[DataType::Int32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Stddev, &[DataType::UInt32])?;
        assert_eq!(DataType::Float64, observed);

        let observed = return_type(&AggregateFunction::Stddev, &[DataType::Int64])?;
        assert_eq!(DataType::Float64, observed);

        Ok(())
    }

    #[test]
    fn test_stddev_no_utf8() {
        let observed = return_type(&AggregateFunction::Stddev, &[DataType::Utf8]);
        assert!(observed.is_err());
    }
}
