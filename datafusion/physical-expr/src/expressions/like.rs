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

use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

use crate::{physical_expr::down_cast_any_ref, PhysicalExpr};

use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::datum::apply_cmp;

// Like expression
#[derive(Debug, Hash)]
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
        use arrow::compute::*;
        let lhs = self.expr.evaluate(batch)?;
        let rhs = self.pattern.evaluate(batch)?;
        match (self.negated, self.case_insensitive) {
            (false, false) => apply_cmp(&lhs, &rhs, like),
            (false, true) => apply_cmp(&lhs, &rhs, ilike),
            (true, false) => apply_cmp(&lhs, &rhs, nlike),
            (true, true) => apply_cmp(&lhs, &rhs, nilike),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.expr, &self.pattern]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(LikeExpr::new(
            self.negated,
            self.case_insensitive,
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
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

/// used for optimize Dictionary like
fn can_like_type(from_type: &DataType) -> bool {
    match from_type {
        DataType::Dictionary(_, inner_type_from) => **inner_type_from == DataType::Utf8,
        _ => false,
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
    if !expr_type.eq(pattern_type) && !can_like_type(expr_type) {
        return internal_err!(
            "The type of {expr_type} AND {pattern_type} of like physical should be same"
        );
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
    use arrow::array::*;
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
            let result = expression
                .evaluate(&batch)?
                .into_array(batch.num_rows())
                .expect("Failed to convert to array");
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
