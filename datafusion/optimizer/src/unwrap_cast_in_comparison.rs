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

//! Unwrap-cast binary comparison rule can be used to the binary/inlist comparison expr now, and other type
//! of expr can be added if needed.
//! This rule can reduce adding the `Expr::Cast` the expr instead of adding the `Expr::Cast` to literal expr.
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{
    DataType, TimeUnit, MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION,
};
use arrow::temporal_conversions::{MICROSECONDS, MILLISECONDS, NANOSECONDS};
use datafusion_common::tree_node::{TreeNodeRecursion, TreeNodeTransformer};
use datafusion_common::{
    internal_err, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{BinaryExpr, Cast, InList, TryCast};
use datafusion_expr::expr_rewriter::rewrite_preserving_name;
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{lit, Expr, ExprSchemable, LogicalPlan, Operator};
use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

/// [`UnwrapCastInComparison`] attempts to remove casts from
/// comparisons to literals ([`ScalarValue`]s) by applying the casts
/// to the literals if possible. It is inspired by the optimizer rule
/// `UnwrapCastInBinaryComparison` of Spark.
///
/// Removing casts often improves performance because:
/// 1. The cast is done once (to the literal) rather than to every value
/// 2. Can enable other optimizations such as predicate pushdown that
///    don't support casting
///
/// The rule is applied to expressions of the following forms:
///
/// 1. `cast(left_expr as data_type) comparison_op literal_expr`
/// 2. `literal_expr comparison_op cast(left_expr as data_type)`
/// 3. `cast(literal_expr) IN (expr1, expr2, ...)`
/// 4. `literal_expr IN (cast(expr1) , cast(expr2), ...)`
///
/// If the expression matches one of the forms above, the rule will
/// ensure the value of `literal` is in range(min, max) of the
/// expr's data_type, and if the scalar is within range, the literal
/// will be casted to the data type of expr on the other side, and the
/// cast will be removed from the other side.
///
/// # Example
///
/// If the DataType of c1 is INT32. Given the filter
///
/// ```text
/// Filter: cast(c1 as INT64) > INT64(10)`
/// ```
///
/// This rule will remove the cast and rewrite the expression to:
///
/// ```text
/// Filter: c1 > INT32(10)
/// ```
///
#[derive(Default)]
pub struct UnwrapCastInComparison {}

impl UnwrapCastInComparison {
    pub fn new() -> Self {
        Self::default()
    }
}

impl OptimizerRule for UnwrapCastInComparison {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let mut schema = merge_schema(plan.inputs());

        if let LogicalPlan::TableScan(ts) = plan {
            let source_schema =
                DFSchema::try_from_qualified_schema(&ts.table_name, &ts.source.schema())?;
            schema.merge(&source_schema);
        }

        schema.merge(plan.schema());

        let mut expr_rewriter = UnwrapCastExprRewriter {
            schema: Arc::new(schema),
        };

        let new_exprs = plan
            .expressions()
            .into_iter()
            .map(|expr| rewrite_preserving_name(expr, &mut expr_rewriter))
            .collect::<Result<Vec<_>>>()?;

        let inputs: Vec<LogicalPlan> = plan.inputs().into_iter().cloned().collect();
        Ok(Some(plan.with_new_exprs(new_exprs, inputs.as_slice())?))
    }

    fn name(&self) -> &str {
        "unwrap_cast_in_comparison"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

struct UnwrapCastExprRewriter {
    schema: DFSchemaRef,
}

impl TreeNodeTransformer for UnwrapCastExprRewriter {
    type Node = Expr;

    fn pre_transform(&mut self, _expr: &mut Expr) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn post_transform(&mut self, expr: &mut Expr) -> Result<TreeNodeRecursion> {
        match expr {
            // For case:
            // try_cast/cast(expr as data_type) op literal
            // literal op try_cast/cast(expr as data_type)
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let left_type = left.get_type(&self.schema)?;
                let right_type = right.get_type(&self.schema)?;
                // Because the plan has been done the type coercion, the left and right must be equal
                if is_support_data_type(&left_type)
                    && is_support_data_type(&right_type)
                    && is_comparison_op(op)
                {
                    match (left.as_mut(), right.as_mut()) {
                        (
                            Expr::Literal(left_lit_value),
                            Expr::TryCast(TryCast { expr, .. })
                            | Expr::Cast(Cast { expr, .. }),
                        ) => {
                            // if the left_lit_value can be casted to the type of expr
                            // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                            let expr_type = expr.get_type(&self.schema)?;
                            let casted_scalar_value =
                                try_cast_literal_to_type(left_lit_value, &expr_type)?;
                            if let Some(value) = casted_scalar_value {
                                // unwrap the cast/try_cast for the right expr
                                **left = lit(value);
                                **right = mem::take(expr.as_mut());
                            }
                        }
                        (
                            Expr::TryCast(TryCast { expr, .. })
                            | Expr::Cast(Cast { expr, .. }),
                            Expr::Literal(right_lit_value),
                        ) => {
                            // if the right_lit_value can be casted to the type of expr
                            // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                            let expr_type = expr.get_type(&self.schema)?;
                            let casted_scalar_value =
                                try_cast_literal_to_type(right_lit_value, &expr_type)?;
                            if let Some(value) = casted_scalar_value {
                                // unwrap the cast/try_cast for the left expr
                                **left = mem::take(expr.as_mut());
                                **right = lit(value);
                            }
                        }
                        (_, _) => {
                            // do nothing
                        }
                    }
                }
            }
            // For case:
            // try_cast/cast(expr as left_type) in (expr1,expr2,expr3)
            Expr::InList(InList {
                expr: left_expr,
                list,
                ..
            }) => {
                if let Expr::TryCast(TryCast {
                    expr: internal_left_expr,
                    ..
                })
                | Expr::Cast(Cast {
                    expr: internal_left_expr,
                    ..
                }) = left_expr.as_ref()
                {
                    let internal_left = internal_left_expr.as_ref().clone();
                    let internal_left_type = internal_left.get_type(&self.schema);
                    if internal_left_type.is_err() {
                        // error data type
                        return Ok(TreeNodeRecursion::Continue);
                    }
                    let internal_left_type = internal_left_type?;
                    if !is_support_data_type(&internal_left_type) {
                        // not supported data type
                        return Ok(TreeNodeRecursion::Continue);
                    }
                    let right_exprs = list
                        .iter()
                        .map(|right| {
                            let right_type = right.get_type(&self.schema)?;
                            if !is_support_data_type(&right_type) {
                                return internal_err!(
                                    "The type of list expr {} not support",
                                    &right_type
                                );
                            }
                            match right {
                                Expr::Literal(right_lit_value) => {
                                    // if the right_lit_value can be casted to the type of internal_left_expr
                                    // we need to unwrap the cast for cast/try_cast expr, and add cast to the literal
                                    let casted_scalar_value =
                                        try_cast_literal_to_type(right_lit_value, &internal_left_type)?;
                                    if let Some(value) = casted_scalar_value {
                                        Ok(lit(value))
                                    } else {
                                        internal_err!(
                                            "Can't cast the list expr {:?} to type {:?}",
                                            right_lit_value, &internal_left_type
                                        )
                                    }
                                }
                                other_expr => internal_err!(
                                    "Only support literal expr to optimize, but the expr is {:?}",
                                    &other_expr
                                ),
                            }
                        })
                        .collect::<Result<Vec<_>>>();
                    if let Ok(right_exprs) = right_exprs {
                        **left_expr = internal_left;
                        *list = right_exprs;
                    }
                }
            }
            // TODO: handle other expr type and dfs visit them
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

fn is_comparison_op(op: &Operator) -> bool {
    matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
    )
}

fn is_support_data_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
    )
}

fn try_cast_literal_to_type(
    lit_value: &ScalarValue,
    target_type: &DataType,
) -> Result<Option<ScalarValue>> {
    let lit_data_type = lit_value.data_type();
    // the rule just support the signed numeric data type now
    if !is_support_data_type(&lit_data_type) || !is_support_data_type(target_type) {
        return Ok(None);
    }
    if lit_value.is_null() {
        // null value can be cast to any type of null value
        return Ok(Some(ScalarValue::try_from(target_type)?));
    }
    let mul = match target_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => 1_i128,
        DataType::Timestamp(_, _) => 1_i128,
        DataType::Decimal128(_, scale) => 10_i128.pow(*scale as u32),
        other_type => {
            return internal_err!("Error target data type {other_type:?}");
        }
    };
    let (target_min, target_max) = match target_type {
        DataType::UInt8 => (u8::MIN as i128, u8::MAX as i128),
        DataType::UInt16 => (u16::MIN as i128, u16::MAX as i128),
        DataType::UInt32 => (u32::MIN as i128, u32::MAX as i128),
        DataType::UInt64 => (u64::MIN as i128, u64::MAX as i128),
        DataType::Int8 => (i8::MIN as i128, i8::MAX as i128),
        DataType::Int16 => (i16::MIN as i128, i16::MAX as i128),
        DataType::Int32 => (i32::MIN as i128, i32::MAX as i128),
        DataType::Int64 => (i64::MIN as i128, i64::MAX as i128),
        DataType::Timestamp(_, _) => (i64::MIN as i128, i64::MAX as i128),
        DataType::Decimal128(precision, _) => (
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            MIN_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
            MAX_DECIMAL_FOR_EACH_PRECISION[*precision as usize - 1],
        ),
        other_type => {
            return internal_err!("Error target data type {other_type:?}");
        }
    };
    let lit_value_target_type = match lit_value {
        ScalarValue::Int8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::Int64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt8(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt16(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt32(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::UInt64(Some(v)) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampSecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMillisecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampMicrosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::TimestampNanosecond(Some(v), _) => (*v as i128).checked_mul(mul),
        ScalarValue::Decimal128(Some(v), _, scale) => {
            let lit_scale_mul = 10_i128.pow(*scale as u32);
            if mul >= lit_scale_mul {
                // Example:
                // lit is decimal(123,3,2)
                // target type is decimal(5,3)
                // the lit can be converted to the decimal(1230,5,3)
                (*v).checked_mul(mul / lit_scale_mul)
            } else if (*v) % (lit_scale_mul / mul) == 0 {
                // Example:
                // lit is decimal(123000,10,3)
                // target type is int32: the lit can be converted to INT32(123)
                // target type is decimal(10,2): the lit can be converted to decimal(12300,10,2)
                Some(*v / (lit_scale_mul / mul))
            } else {
                // can't convert the lit decimal to the target data type
                None
            }
        }
        other_value => {
            return internal_err!("Invalid literal value {other_value:?}");
        }
    };

    match lit_value_target_type {
        None => Ok(None),
        Some(value) => {
            if value >= target_min && value <= target_max {
                // the value casted from lit to the target type is in the range of target type.
                // return the target type of scalar value
                let result_scalar = match target_type {
                    DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
                    DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
                    DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
                    DataType::Int64 => ScalarValue::Int64(Some(value as i64)),
                    DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
                    DataType::Timestamp(TimeUnit::Second, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Second, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampSecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Millisecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMillisecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampMicrosecond(value, tz.clone())
                    }
                    DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                        let value = cast_between_timestamp(
                            lit_data_type,
                            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone()),
                            value,
                        );
                        ScalarValue::TimestampNanosecond(value, tz.clone())
                    }
                    DataType::Decimal128(p, s) => {
                        ScalarValue::Decimal128(Some(value), *p, *s)
                    }
                    other_type => {
                        return internal_err!("Error target data type {other_type:?}");
                    }
                };
                Ok(Some(result_scalar))
            } else {
                Ok(None)
            }
        }
    }
}

/// Cast a timestamp value from one unit to another
fn cast_between_timestamp(from: DataType, to: DataType, value: i128) -> Option<i64> {
    let value = value as i64;
    let from_scale = match from {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    let to_scale = match to {
        DataType::Timestamp(TimeUnit::Second, _) => 1,
        DataType::Timestamp(TimeUnit::Millisecond, _) => MILLISECONDS,
        DataType::Timestamp(TimeUnit::Microsecond, _) => MICROSECONDS,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => NANOSECONDS,
        _ => return Some(value),
    };

    match from_scale.cmp(&to_scale) {
        Ordering::Less => value.checked_mul(to_scale / from_scale),
        Ordering::Greater => Some(value / (from_scale / to_scale)),
        Ordering::Equal => Some(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::unwrap_cast_in_comparison::UnwrapCastExprRewriter;
    use arrow::compute::{cast_with_options, CastOptions};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::tree_node::TreeNode;
    use datafusion_common::{DFField, DFSchema, DFSchemaRef, ScalarValue};
    use datafusion_expr::{cast, col, in_list, lit, try_cast, Expr};
    use std::collections::HashMap;
    use std::sync::Arc;

    #[test]
    fn test_not_unwrap_cast_comparison() {
        let schema = expr_test_schema();
        // cast(INT32(c1), INT64) > INT64(c2)
        let c1_gt_c2 = cast(col("c1"), DataType::Int64).gt(col("c2"));
        assert_eq!(optimize_test(c1_gt_c2.clone(), &schema), c1_gt_c2);

        // INT32(c1) < INT32(16), the type is same
        let expr_lt = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // the 99999999999 is not within the range of MAX(int32) and MIN(int32), we don't cast the lit(99999999999) to int32 type
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(99999999999i64));
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_unwrap_cast_comparison() {
        let schema = expr_test_schema();
        // cast(c1, INT64) < INT64(16) -> INT32(c1) < cast(INT32(16))
        // the 16 is within the range of MAX(int32) and MIN(int32), we can cast the 16 to int32(16)
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        let expr_lt = try_cast(col("c1"), DataType::Int64).lt(lit(16i64));
        let expected = col("c1").lt(lit(16i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // cast(c2, INT32) = INT32(16) => INT64(c2) = INT64(16)
        let c2_eq_lit = cast(col("c2"), DataType::Int32).eq(lit(16i32));
        let expected = col("c2").eq(lit(16i64));
        assert_eq!(optimize_test(c2_eq_lit, &schema), expected);

        // cast(c1, INT64) < INT64(NULL) => INT32(c1) < INT32(NULL)
        let c1_lt_lit_null = cast(col("c1"), DataType::Int64).lt(null_i64());
        let expected = col("c1").lt(null_i32());
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // cast(INT8(NULL), INT32) < INT32(12) => INT8(NULL) < INT8(12)
        let lit_lt_lit = cast(null_i8(), DataType::Int32).lt(lit(12i32));
        let expected = null_i8().lt(lit(12i8));
        assert_eq!(optimize_test(lit_lt_lit, &schema), expected);
    }

    #[test]
    fn test_unwrap_cast_comparison_unsigned() {
        // "cast(c6, UINT64) = 0u64 => c6 = 0u32
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::UInt64).eq(lit(0u64));
        let expected = col("c6").eq(lit(0u32));
        assert_eq!(optimize_test(expr_input, &schema), expected);
    }

    #[test]
    fn test_not_unwrap_cast_with_decimal_comparison() {
        let schema = expr_test_schema();
        // integer to decimal: value is out of the bounds of the decimal
        // cast(c3, INT64) = INT64(100000000000000000)
        let expr_eq = cast(col("c3"), DataType::Int64).eq(lit(100000000000000000i64));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // cast(c4, INT64) = INT64(1000) will overflow the i128
        let expr_eq = cast(col("c4"), DataType::Int64).eq(lit(1000i64));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // decimal to decimal: value will lose the scale when convert to the target data type
        // c3 = DECIMAL(12340,20,4)
        let expr_eq =
            cast(col("c3"), DataType::Decimal128(20, 4)).eq(lit_decimal(12340, 20, 4));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // decimal to integer
        // c1 = DECIMAL(123, 10, 1): value will lose the scale when convert to the target data type
        let expr_eq =
            cast(col("c1"), DataType::Decimal128(10, 1)).eq(lit_decimal(123, 10, 1));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);

        // c1 = DECIMAL(1230, 10, 2): value will lose the scale when convert to the target data type
        let expr_eq =
            cast(col("c1"), DataType::Decimal128(10, 2)).eq(lit_decimal(1230, 10, 2));
        assert_eq!(optimize_test(expr_eq.clone(), &schema), expr_eq);
    }

    #[test]
    fn test_unwrap_cast_with_decimal_lit_comparison() {
        let schema = expr_test_schema();
        // integer to decimal
        // c3 < INT64(16) -> c3 < (CAST(INT64(16) AS DECIMAL(18,2));
        let expr_lt = try_cast(col("c3"), DataType::Int64).lt(lit(16i64));
        let expected = col("c3").lt(lit_decimal(1600, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // c3 < INT64(NULL)
        let c1_lt_lit_null = cast(col("c3"), DataType::Int64).lt(null_i64());
        let expected = col("c3").lt(null_decimal(18, 2));
        assert_eq!(optimize_test(c1_lt_lit_null, &schema), expected);

        // decimal to decimal
        // c3 < Decimal(123,10,0) -> c3 < CAST(DECIMAL(123,10,0) AS DECIMAL(18,2)) -> c3 < DECIMAL(12300,18,2)
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 0)).lt(lit_decimal(123, 10, 0));
        let expected = col("c3").lt(lit_decimal(12300, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // c3 < Decimal(1230,10,3) -> c3 < CAST(DECIMAL(1230,10,3) AS DECIMAL(18,2)) -> c3 < DECIMAL(123,18,2)
        let expr_lt =
            cast(col("c3"), DataType::Decimal128(10, 3)).lt(lit_decimal(1230, 10, 3));
        let expected = col("c3").lt(lit_decimal(123, 18, 2));
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // decimal to integer
        // c1 < Decimal(12300, 10, 2) -> c1 < CAST(DECIMAL(12300,10,2) AS INT32) -> c1 < INT32(123)
        let expr_lt =
            cast(col("c1"), DataType::Decimal128(10, 2)).lt(lit_decimal(12300, 10, 2));
        let expected = col("c1").lt(lit(123i32));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn test_not_unwrap_list_cast_lit_comparison() {
        let schema = expr_test_schema();
        // internal left type is not supported
        // FLOAT32(C5) in ...
        let expr_lt =
            cast(col("c5"), DataType::Int64).in_list(vec![lit(12i64), lit(12i64)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // cast(INT32(C1), Float32) in (FLOAT32(1.23), Float32(12), Float32(12))
        let expr_lt = cast(col("c1"), DataType::Float32)
            .in_list(vec![lit(12.0f32), lit(12.0f32), lit(1.23f32)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // INT32(C1) in (INT64(99999999999), INT64(12))
        let expr_lt = cast(col("c1"), DataType::Int64)
            .in_list(vec![lit(12i32), lit(99999999999i64)], false);
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);

        // DECIMAL(C3) in (INT64(12), INT32(12), DECIMAL(128,12,3))
        let expr_lt = cast(col("c3"), DataType::Decimal128(12, 3)).in_list(
            vec![
                lit_decimal(12, 12, 3),
                lit_decimal(12, 12, 3),
                lit_decimal(128, 12, 3),
            ],
            false,
        );
        assert_eq!(optimize_test(expr_lt.clone(), &schema), expr_lt);
    }

    #[test]
    fn test_unwrap_list_cast_comparison() {
        let schema = expr_test_schema();
        // INT32(C1) IN (INT32(12),INT64(24)) -> INT32(C1) IN (INT32(12),INT32(24))
        let expr_lt =
            cast(col("c1"), DataType::Int64).in_list(vec![lit(12i64), lit(24i64)], false);
        let expected = col("c1").in_list(vec![lit(12i32), lit(24i32)], false);
        assert_eq!(optimize_test(expr_lt, &schema), expected);
        // INT32(C2) IN (INT64(NULL),INT64(24)) -> INT32(C1) IN (INT32(12),INT32(24))
        let expr_lt =
            cast(col("c2"), DataType::Int32).in_list(vec![null_i32(), lit(14i32)], false);
        let expected = col("c2").in_list(vec![null_i64(), lit(14i64)], false);

        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // decimal test case
        // c3 is decimal(18,2)
        let expr_lt = cast(col("c3"), DataType::Decimal128(19, 3)).in_list(
            vec![
                lit_decimal(12000, 19, 3),
                lit_decimal(24000, 19, 3),
                lit_decimal(1280, 19, 3),
                lit_decimal(1240, 19, 3),
            ],
            false,
        );
        let expected = col("c3").in_list(
            vec![
                lit_decimal(1200, 18, 2),
                lit_decimal(2400, 18, 2),
                lit_decimal(128, 18, 2),
                lit_decimal(124, 18, 2),
            ],
            false,
        );
        assert_eq!(optimize_test(expr_lt, &schema), expected);

        // cast(INT32(12), INT64) IN (.....)
        let expr_lt = cast(lit(12i32), DataType::Int64)
            .in_list(vec![lit(13i64), lit(12i64)], false);
        let expected = lit(12i32).in_list(vec![lit(13i32), lit(12i32)], false);
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn aliased() {
        let schema = expr_test_schema();
        // c1 < INT64(16) -> c1 < cast(INT32(16))
        // the 16 is within the range of MAX(int32) and MIN(int32), we can cast the 16 to int32(16)
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64)).alias("x");
        let expected = col("c1").lt(lit(16i32)).alias("x");
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn nested() {
        let schema = expr_test_schema();
        // c1 < INT64(16) OR c1 > INT64(32) -> c1 < INT32(16) OR c1 > INT32(32)
        // the 16 and 32 are within the range of MAX(int32) and MIN(int32), we can cast them to int32
        let expr_lt = cast(col("c1"), DataType::Int64).lt(lit(16i64)).or(cast(
            col("c1"),
            DataType::Int64,
        )
        .gt(lit(32i64)));
        let expected = col("c1").lt(lit(16i32)).or(col("c1").gt(lit(32i32)));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    #[test]
    fn test_not_support_data_type() {
        // "c6 > 0" will be cast to `cast(c6 as float) > 0
        // but the type of c6 is uint32
        // the rewriter will not throw error and just return the original expr
        let schema = expr_test_schema();
        let expr_input = cast(col("c6"), DataType::Float64).eq(lit(0f64));
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);

        // inlist for unsupported data type
        let expr_input =
            in_list(cast(col("c6"), DataType::Float64), vec![lit(0f64)], false);
        assert_eq!(optimize_test(expr_input.clone(), &schema), expr_input);
    }

    #[test]
    /// Basic integration test for unwrapping casts with different timezones
    fn test_unwrap_cast_with_timestamp_nanos() {
        let schema = expr_test_schema();
        // cast(ts_nano as Timestamp(Nanosecond, UTC)) < 1666612093000000000::Timestamp(Nanosecond, Utc))
        let expr_lt = try_cast(col("ts_nano_none"), timestamp_nano_utc_type())
            .lt(lit_timestamp_nano_utc(1666612093000000000));
        let expected =
            col("ts_nano_none").lt(lit_timestamp_nano_none(1666612093000000000));
        assert_eq!(optimize_test(expr_lt, &schema), expected);
    }

    fn optimize_test(mut expr: Expr, schema: &DFSchemaRef) -> Expr {
        let mut expr_rewriter = UnwrapCastExprRewriter {
            schema: schema.clone(),
        };
        expr.transform(&mut expr_rewriter).unwrap();
        expr
    }

    fn expr_test_schema() -> DFSchemaRef {
        Arc::new(
            DFSchema::new_with_metadata(
                vec![
                    DFField::new_unqualified("c1", DataType::Int32, false),
                    DFField::new_unqualified("c2", DataType::Int64, false),
                    DFField::new_unqualified("c3", DataType::Decimal128(18, 2), false),
                    DFField::new_unqualified("c4", DataType::Decimal128(38, 37), false),
                    DFField::new_unqualified("c5", DataType::Float32, false),
                    DFField::new_unqualified("c6", DataType::UInt32, false),
                    DFField::new_unqualified(
                        "ts_nano_none",
                        timestamp_nano_none_type(),
                        false,
                    ),
                    DFField::new_unqualified(
                        "ts_nano_utf",
                        timestamp_nano_utc_type(),
                        false,
                    ),
                ],
                HashMap::new(),
            )
            .unwrap(),
        )
    }

    fn null_i8() -> Expr {
        lit(ScalarValue::Int8(None))
    }

    fn null_i32() -> Expr {
        lit(ScalarValue::Int32(None))
    }

    fn null_i64() -> Expr {
        lit(ScalarValue::Int64(None))
    }

    fn lit_decimal(value: i128, precision: u8, scale: i8) -> Expr {
        lit(ScalarValue::Decimal128(Some(value), precision, scale))
    }

    fn lit_timestamp_nano_none(ts: i64) -> Expr {
        lit(ScalarValue::TimestampNanosecond(Some(ts), None))
    }

    fn lit_timestamp_nano_utc(ts: i64) -> Expr {
        let utc = Some("+0:00".into());
        lit(ScalarValue::TimestampNanosecond(Some(ts), utc))
    }

    fn null_decimal(precision: u8, scale: i8) -> Expr {
        lit(ScalarValue::Decimal128(None, precision, scale))
    }

    fn timestamp_nano_none_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }

    // this is the type that now() returns
    fn timestamp_nano_utc_type() -> DataType {
        let utc = Some("+0:00".into());
        DataType::Timestamp(TimeUnit::Nanosecond, utc)
    }

    #[test]
    fn test_try_cast_to_type_nulls() {
        // test that nulls can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Decimal128(None, 3, 0),
            ScalarValue::Decimal128(None, 8, 2),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }
    }

    #[test]
    fn test_try_cast_to_type_int_in_range() {
        // test values that can be cast to/from all integer types
        let scalars = vec![
            ScalarValue::Int8(Some(123)),
            ScalarValue::Int16(Some(123)),
            ScalarValue::Int32(Some(123)),
            ScalarValue::Int64(Some(123)),
            ScalarValue::UInt8(Some(123)),
            ScalarValue::UInt16(Some(123)),
            ScalarValue::UInt32(Some(123)),
            ScalarValue::UInt64(Some(123)),
            ScalarValue::Decimal128(Some(123), 3, 0),
            ScalarValue::Decimal128(Some(12300), 8, 2),
        ];

        for s1 in &scalars {
            for s2 in &scalars {
                let expected_value = ExpectedCast::Value(s2.clone());

                expect_cast(s1.clone(), s2.data_type(), expected_value);
            }
        }

        let max_i32 = ScalarValue::Int32(Some(i32::MAX));
        expect_cast(
            max_i32,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i32::MAX as u64))),
        );

        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        expect_cast(
            min_i32,
            DataType::Int64,
            ExpectedCast::Value(ScalarValue::Int64(Some(i32::MIN as i64))),
        );

        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        expect_cast(
            max_i64,
            DataType::UInt64,
            ExpectedCast::Value(ScalarValue::UInt64(Some(i64::MAX as u64))),
        );
    }

    #[test]
    fn test_try_cast_to_type_int_out_of_range() {
        let min_i32 = ScalarValue::Int32(Some(i32::MIN));
        let min_i64 = ScalarValue::Int64(Some(i64::MIN));
        let max_i64 = ScalarValue::Int64(Some(i64::MAX));
        let max_u64 = ScalarValue::UInt64(Some(u64::MAX));

        expect_cast(max_i64.clone(), DataType::Int8, ExpectedCast::NoValue);

        expect_cast(max_i64.clone(), DataType::Int16, ExpectedCast::NoValue);

        expect_cast(max_i64, DataType::Int32, ExpectedCast::NoValue);

        expect_cast(max_u64, DataType::Int64, ExpectedCast::NoValue);

        expect_cast(min_i64, DataType::UInt64, ExpectedCast::NoValue);

        expect_cast(min_i32, DataType::UInt64, ExpectedCast::NoValue);

        // decimal out of range
        expect_cast(
            ScalarValue::Decimal128(Some(99999999999999999999999999999999999900), 38, 0),
            DataType::Int64,
            ExpectedCast::NoValue,
        );

        expect_cast(
            ScalarValue::Decimal128(Some(-9999999999999999999999999999999999), 37, 1),
            DataType::Int64,
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_decimal_cast_in_range() {
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 3, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 0),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(123), 8, 0)),
        );

        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(8, 5),
            ExpectedCast::Value(ScalarValue::Decimal128(Some(12300000), 8, 5)),
        );
    }

    #[test]
    fn test_try_decimal_cast_out_of_range() {
        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12345), 5, 2),
            DataType::Decimal128(3, 0),
            ExpectedCast::NoValue,
        );

        // decimal would lose precision
        expect_cast(
            ScalarValue::Decimal128(Some(12300), 5, 2),
            DataType::Decimal128(2, 0),
            ExpectedCast::NoValue,
        );
    }

    #[test]
    fn test_try_cast_to_type_timestamps() {
        for time_unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            let utc = Some("+00:00".into());
            // No timezone, utc timezone
            let (lit_tz_none, lit_tz_utc) = match time_unit {
                TimeUnit::Second => (
                    ScalarValue::TimestampSecond(Some(12345), None),
                    ScalarValue::TimestampSecond(Some(12345), utc),
                ),

                TimeUnit::Millisecond => (
                    ScalarValue::TimestampMillisecond(Some(12345), None),
                    ScalarValue::TimestampMillisecond(Some(12345), utc),
                ),

                TimeUnit::Microsecond => (
                    ScalarValue::TimestampMicrosecond(Some(12345), None),
                    ScalarValue::TimestampMicrosecond(Some(12345), utc),
                ),

                TimeUnit::Nanosecond => (
                    ScalarValue::TimestampNanosecond(Some(12345), None),
                    ScalarValue::TimestampNanosecond(Some(12345), utc),
                ),
            };

            // Datafusion ignores timezones for comparisons of ScalarValue
            // so double check it here
            assert_eq!(lit_tz_none, lit_tz_utc);

            // e.g. DataType::Timestamp(_, None)
            let dt_tz_none = lit_tz_none.data_type();

            // e.g. DataType::Timestamp(_, Some(utc))
            let dt_tz_utc = lit_tz_utc.data_type();

            // None <--> None
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // None <--> Utc
            expect_cast(
                lit_tz_none.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // Utc <--> None
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // Utc <--> Utc
            expect_cast(
                lit_tz_utc.clone(),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to int64
            expect_cast(
                lit_tz_utc.clone(),
                DataType::Int64,
                ExpectedCast::Value(ScalarValue::Int64(Some(12345))),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_none.clone(),
                ExpectedCast::Value(lit_tz_none.clone()),
            );

            // int64 to timestamp
            expect_cast(
                ScalarValue::Int64(Some(12345)),
                dt_tz_utc.clone(),
                ExpectedCast::Value(lit_tz_utc.clone()),
            );

            // timestamp to string (not supported yet)
            expect_cast(
                lit_tz_utc.clone(),
                DataType::LargeUtf8,
                ExpectedCast::NoValue,
            );
        }
    }

    #[test]
    fn test_try_cast_to_type_unsupported() {
        // int64 to list
        expect_cast(
            ScalarValue::Int64(Some(12345)),
            DataType::List(Arc::new(Field::new("f", DataType::Int32, true))),
            ExpectedCast::NoValue,
        );
    }

    #[derive(Debug, Clone)]
    enum ExpectedCast {
        /// test successfully cast value and it is as specified
        Value(ScalarValue),
        /// test returned OK, but could not cast the value
        NoValue,
    }

    /// Runs try_cast_literal_to_type with the specified inputs and
    /// ensure it computes the expected output, and ensures the
    /// casting is consistent with the Arrow kernels
    fn expect_cast(
        literal: ScalarValue,
        target_type: DataType,
        expected_result: ExpectedCast,
    ) {
        let actual_result = try_cast_literal_to_type(&literal, &target_type);

        println!("expect_cast: ");
        println!("  {literal:?} --> {target_type:?}");
        println!("  expected_result: {expected_result:?}");
        println!("  actual_result:   {actual_result:?}");

        match expected_result {
            ExpectedCast::Value(expected_value) => {
                let actual_value = actual_result
                    .expect("Expected success but got error")
                    .expect("Expected cast value but got None");

                assert_eq!(actual_value, expected_value);

                // Verify that calling the arrow
                // cast kernel yields the same results
                // input array
                let literal_array = literal
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let expected_array = expected_value
                    .to_array_of_size(1)
                    .expect("Failed to convert to array of size");
                let cast_array = cast_with_options(
                    &literal_array,
                    &target_type,
                    &CastOptions::default(),
                )
                .expect("Expected to be cast array with arrow cast kernel");

                assert_eq!(
                    &expected_array, &cast_array,
                    "Result of casing {literal:?} with arrow was\n {cast_array:#?}\nbut expected\n{expected_array:#?}"
                );

                // Verify that for timestamp types the timezones are the same
                // (ScalarValue::cmp doesn't account for timezones);
                if let (
                    DataType::Timestamp(left_unit, left_tz),
                    DataType::Timestamp(right_unit, right_tz),
                ) = (actual_value.data_type(), expected_value.data_type())
                {
                    assert_eq!(left_unit, right_unit);
                    assert_eq!(left_tz, right_tz);
                }
            }
            ExpectedCast::NoValue => {
                let actual_value = actual_result.expect("Expected success but got error");

                assert!(
                    actual_value.is_none(),
                    "Expected no cast value, but got {actual_value:?}"
                );
            }
        }
    }

    #[test]
    fn test_try_cast_literal_to_timestamp() {
        // same timestamp
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123456), None)
        );

        // TimestampNanosecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123), None)
        );

        // TimestampNanosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampNanosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampNanosecond(Some(123456), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(0), None));

        // TimestampMicrosecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000), None)
        );

        // TimestampMicrosecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap()
        .unwrap();

        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(Some(0), None));

        // TimestampMicrosecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMicrosecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123), None));

        // TimestampMillisecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000), None)
        );

        // TimestampMillisecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000), None)
        );
        // TimestampMillisecond to TimestampSecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampMillisecond(Some(123456789), None),
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampSecond(Some(123456), None));

        // TimestampSecond to TimestampNanosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampNanosecond(Some(123000000000), None)
        );

        // TimestampSecond to TimestampMicrosecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMicrosecond(Some(123000000), None)
        );

        // TimestampSecond to TimestampMillisecond
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(123), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            new_scalar,
            ScalarValue::TimestampMillisecond(Some(123000), None)
        );

        // overflow
        let new_scalar = try_cast_literal_to_type(
            &ScalarValue::TimestampSecond(Some(i64::MAX), None),
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(new_scalar, ScalarValue::TimestampMillisecond(None, None));
    }
}
