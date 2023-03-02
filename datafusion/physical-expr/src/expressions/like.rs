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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{new_null_array, Array, ArrayRef, StringArray},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

use crate::{physical_expr::down_cast_any_ref, AnalysisContext, PhysicalExpr};

use arrow::compute::kernels::comparison::{
    ilike_utf8, like_utf8, nilike_utf8, nlike_utf8,
};
use arrow::compute::kernels::comparison::{
    ilike_utf8_scalar_dyn, like_utf8_scalar_dyn, nilike_utf8_scalar_dyn,
    nlike_utf8_scalar_dyn,
};
use datafusion_expr::type_coercion::binary::like_coercion;

// Like expression
#[derive(Debug)]
pub struct LikeExpr {
    negated: bool,
    case_insensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
}

impl LikeExpr {
    pub fn new(
        negated: bool,
        case_insensitive: bool,
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            negated,
            case_insensitive,
            expr,
            pattern,
        }
    }

    /// Is negated
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Is case insensitive
    pub fn case_insensitive(&self) -> bool {
        self.case_insensitive
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Pattern expression
    pub fn pattern(&self) -> &Arc<dyn PhysicalExpr> {
        &self.pattern
    }

    /// Operator name
    fn op_name(&self) -> &str {
        match (self.negated, self.case_insensitive) {
            (false, false) => "LIKE",
            (true, false) => "NOT LIKE",
            (false, true) => "ILIKE",
            (true, true) => "NOT ILIKE",
        }
    }
}

impl std::fmt::Display for LikeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.op_name(), self.pattern)
    }
}

impl PhysicalExpr for LikeExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.expr.nullable(input_schema)? || self.pattern.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let expr_value = self.expr.evaluate(batch)?;
        let pattern_value = self.pattern.evaluate(batch)?;
        let expr_data_type = expr_value.data_type();
        let pattern_data_type = pattern_value.data_type();

        match (
            &expr_value,
            &expr_data_type,
            &pattern_value,
            &pattern_data_type,
        ) {
            // Types are equal => valid
            (_, l, _, r) if l == r => {}
            // Allow comparing a dictionary value with its corresponding scalar value
            (
                ColumnarValue::Array(_),
                DataType::Dictionary(_, dict_t),
                ColumnarValue::Scalar(_),
                scalar_t,
            )
            | (
                ColumnarValue::Scalar(_),
                scalar_t,
                ColumnarValue::Array(_),
                DataType::Dictionary(_, dict_t),
            ) if dict_t.as_ref() == scalar_t => {}
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Cannot evaluate {} expression with types {:?} and {:?}",
                    self.op_name(),
                    expr_data_type,
                    pattern_data_type
                )));
            }
        }

        // Attempt to use special kernels if one input is scalar and the other is an array
        let scalar_result = match (&expr_value, &pattern_value) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                self.evaluate_array_scalar(array, scalar)?
            }
            (_, _) => None, // default to array implementation
        };

        if let Some(result) = scalar_result {
            return result.map(|a| ColumnarValue::Array(a));
        }

        // if both arrays or both literals - extract arrays and continue execution
        let (expr, pattern) = (
            expr_value.into_array(batch.num_rows()),
            pattern_value.into_array(batch.num_rows()),
        );
        self.evaluate_array_array(expr, pattern)
            .map(|a| ColumnarValue::Array(a))
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone(), self.pattern.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(LikeExpr::new(
            self.negated,
            self.case_insensitive,
            children[0].clone(),
            children[1].clone(),
        )))
    }

    /// Return the boundaries of this binary expression's result.
    fn analyze(&self, context: AnalysisContext) -> AnalysisContext {
        context.with_boundaries(None)
    }
}

impl PartialEq<dyn Any> for LikeExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.negated == x.negated
                    && self.case_insensitive == x.case_insensitive
                    && self.expr.eq(&x.expr)
                    && self.pattern.eq(&x.pattern)
            })
            .unwrap_or(false)
    }
}

macro_rules! binary_string_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $OP_TYPE:expr) => {{
        let result: Result<Arc<dyn Array>> =
            compute_utf8_op_scalar_dyn!($LEFT, $RIGHT, $OP, $OP_TYPE);
        Some(result)
    }};
}

impl LikeExpr {
    /// Evaluate the expression if the input is an array and
    /// pattern is literal - use scalar operations
    fn evaluate_array_scalar(
        &self,
        array: &dyn Array,
        scalar: &ScalarValue,
    ) -> Result<Option<Result<ArrayRef>>> {
        let scalar_result = match (self.negated, self.case_insensitive) {
            (false, false) => binary_string_array_op_scalar!(
                array,
                scalar.clone(),
                like,
                &DataType::Boolean
            ),
            (true, false) => binary_string_array_op_scalar!(
                array,
                scalar.clone(),
                nlike,
                &DataType::Boolean
            ),
            (false, true) => binary_string_array_op_scalar!(
                array,
                scalar.clone(),
                ilike,
                &DataType::Boolean
            ),
            (true, true) => binary_string_array_op_scalar!(
                array,
                scalar.clone(),
                nilike,
                &DataType::Boolean
            ),
        };
        Ok(scalar_result)
    }

    fn evaluate_array_array(
        &self,
        left: Arc<dyn Array>,
        right: Arc<dyn Array>,
    ) -> Result<ArrayRef> {
        match (self.negated, self.case_insensitive) {
            (false, false) => binary_string_array_op!(left, right, like),
            (true, false) => binary_string_array_op!(left, right, nlike),
            (false, true) => binary_string_array_op!(left, right, ilike),
            (true, true) => binary_string_array_op!(left, right, nilike),
        }
    }
}

/// Create a like expression, erroring if the argument types are not compatible.
pub fn like(
    negated: bool,
    case_insensitive: bool,
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = &expr.data_type(input_schema)?;
    let pattern_type = &pattern.data_type(input_schema)?;
    if like_coercion(expr_type, pattern_type).is_none() {
        return Err(DataFusionError::Internal(format!(
            "The type of {expr_type} AND {pattern_type} of like physical must be coercible"
        )));
    }
    Ok(Arc::new(LikeExpr::new(
        negated,
        case_insensitive,
        expr,
        pattern,
    )))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expressions::col;
    use arrow::array::BooleanArray;
    use arrow_schema::Field;
    use datafusion_common::cast::as_boolean_array;

    macro_rules! test_like {
        ($A_VEC:expr, $B_VEC:expr, $VEC:expr, $NULLABLE: expr, $NEGATED:expr, $CASE_INSENSITIVE:expr,) => {{
            let schema = Schema::new(vec![
                Field::new("a", DataType::Utf8, $NULLABLE),
                Field::new("b", DataType::Utf8, $NULLABLE),
            ]);
            let a = StringArray::from($A_VEC);
            let b = StringArray::from($B_VEC);

            let expression = like(
                $NEGATED,
                $CASE_INSENSITIVE,
                col("a", &schema)?,
                col("b", &schema)?,
                &schema,
            )?;
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(a), Arc::new(b)],
            )?;

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
            let result =
                as_boolean_array(&result).expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($VEC);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn like_op() -> Result<()> {
        test_like!(
            vec!["hello world", "world"],
            vec!["%hello%", "%hello%"],
            vec![true, false],
            false,
            false,
            false,
        ); // like
        test_like!(
            vec![Some("hello world"), None, Some("world")],
            vec![Some("%hello%"), None, Some("%hello%")],
            vec![Some(false), None, Some(true)],
            true,
            true,
            false,
        ); // not like
        test_like!(
            vec!["hello world", "world"],
            vec!["%helLo%", "%helLo%"],
            vec![true, false],
            false,
            false,
            true,
        ); // ilike
        test_like!(
            vec![Some("hello world"), None, Some("world")],
            vec![Some("%helLo%"), None, Some("%helLo%")],
            vec![Some(false), None, Some(true)],
            true,
            true,
            true,
        ); // not ilike

        Ok(())
    }
}
