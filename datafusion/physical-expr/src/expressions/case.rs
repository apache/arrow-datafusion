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

use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

use crate::expressions::try_cast;
use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;

use arrow::array::*;
use arrow::compute::kernels::cmp::eq;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, and_not, is_null, not, nullif, or, prep_null_mask_filter};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::{exec_err, internal_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::ColumnarValue;

use itertools::Itertools;

type WhenThen = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

/// The CASE expression is similar to a series of nested if/else and there are two forms that
/// can be used. The first form consists of a series of boolean "when" expressions with
/// corresponding "then" expressions, and an optional "else" expression.
///
/// CASE WHEN condition THEN result
///      [WHEN ...]
///      [ELSE result]
/// END
///
/// The second form uses a base expression and then a series of "when" clauses that match on a
/// literal value.
///
/// CASE expression
///     WHEN value THEN result
///     [WHEN ...]
///     [ELSE result]
/// END
#[derive(Debug, Hash)]
pub struct CaseExpr {
    /// Optional base expression that can be compared to literal values in the "when" expressions
    expr: Option<Arc<dyn PhysicalExpr>>,
    /// One or more when/then expressions
    when_then_expr: Vec<WhenThen>,
    /// Optional "else" expression
    else_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Display for CaseExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CASE ")?;
        if let Some(e) = &self.expr {
            write!(f, "{e} ")?;
        }
        for (w, t) in &self.when_then_expr {
            write!(f, "WHEN {w} THEN {t} ")?;
        }
        if let Some(e) = &self.else_expr {
            write!(f, "ELSE {e} ")?;
        }
        write!(f, "END")
    }
}

impl CaseExpr {
    /// Create a new CASE WHEN expression
    pub fn try_new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_then_expr: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Result<Self> {
        if when_then_expr.is_empty() {
            exec_err!("There must be at least one WHEN clause")
        } else {
            Ok(Self {
                expr,
                when_then_expr,
                else_expr,
            })
        }
    }

    /// Optional base expression that can be compared to literal values in the "when" expressions
    pub fn expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.expr.as_ref()
    }

    /// One or more when/then expressions
    pub fn when_then_expr(&self) -> &[WhenThen] {
        &self.when_then_expr
    }

    /// Optional "else" expression
    pub fn else_expr(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.else_expr.as_ref()
    }
}

impl CaseExpr {
    /// This function evaluates the form of CASE that matches an expression to fixed values.
    ///
    /// CASE expression
    ///     WHEN value THEN result
    ///     [WHEN ...]
    ///     [ELSE result]
    /// END
    fn case_when_with_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;
        let expr = self.expr.as_ref().unwrap();
        let base_value = expr.evaluate(batch)?;
        let base_value = base_value.into_array(batch.num_rows())?;
        let base_nulls = is_null(base_value.as_ref())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        // We only consider non-null values while comparing with whens
        let mut remainder = not(&base_nulls)?;
        for i in 0..self.when_then_expr.len() {
            let when_value = self.when_then_expr[i]
                .0
                .evaluate_selection(batch, &remainder)?;
            let when_value = when_value.into_array(batch.num_rows())?;
            // build boolean array representing which rows match the "when" value
            let when_match = eq(&when_value, &base_value)?;
            // Treat nulls as false
            let when_match = match when_match.null_count() {
                0 => Cow::Borrowed(&when_match),
                _ => Cow::Owned(prep_null_mask_filter(&when_match)),
            };
            // Make sure we only consider rows that have not been matched yet
            let when_match = and(&when_match, &remainder)?;

            // When no rows available for when clause, skip then clause
            if when_match.true_count() == 0 {
                continue;
            }

            let then_value = self.when_then_expr[i]
                .1
                .evaluate_selection(batch, &when_match)?;

            current_value = match then_value {
                ColumnarValue::Scalar(ScalarValue::Null) => {
                    nullif(current_value.as_ref(), &when_match)?
                }
                ColumnarValue::Scalar(then_value) => {
                    zip(&when_match, &then_value.to_scalar()?, &current_value)?
                }
                ColumnarValue::Array(then_value) => {
                    zip(&when_match, &then_value, &current_value)?
                }
            };

            remainder = and_not(&remainder, &when_match)?;
        }

        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())
                .unwrap_or_else(|_| Arc::clone(e));
            // null and unmatched tuples should be assigned else value
            remainder = or(&base_nulls, &remainder)?;
            let else_ = expr
                .evaluate_selection(batch, &remainder)?
                .into_array(batch.num_rows())?;
            current_value = zip(&remainder, &else_, &current_value)?;
        }

        Ok(ColumnarValue::Array(current_value))
    }

    /// This function evaluates the form of CASE where each WHEN expression is a boolean
    /// expression.
    ///
    /// CASE WHEN condition THEN result
    ///      [WHEN ...]
    ///      [ELSE result]
    /// END
    fn case_when_no_expr(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let return_type = self.data_type(&batch.schema())?;

        // start with nulls as default output
        let mut current_value = new_null_array(&return_type, batch.num_rows());
        let mut remainder = BooleanArray::from(vec![true; batch.num_rows()]);
        for i in 0..self.when_then_expr.len() {
            let when_value = self.when_then_expr[i]
                .0
                .evaluate_selection(batch, &remainder)?;
            let when_value = when_value.into_array(batch.num_rows())?;
            let when_value = as_boolean_array(&when_value).map_err(|e| {
                DataFusionError::Context(
                    "WHEN expression did not return a BooleanArray".to_string(),
                    Box::new(e),
                )
            })?;
            // Treat 'NULL' as false value
            let when_value = match when_value.null_count() {
                0 => Cow::Borrowed(when_value),
                _ => Cow::Owned(prep_null_mask_filter(when_value)),
            };
            // Make sure we only consider rows that have not been matched yet
            let when_value = and(&when_value, &remainder)?;

            // When no rows available for when clause, skip then clause
            if when_value.true_count() == 0 {
                continue;
            }

            let then_value = self.when_then_expr[i]
                .1
                .evaluate_selection(batch, &when_value)?;

            current_value = match then_value {
                ColumnarValue::Scalar(ScalarValue::Null) => {
                    nullif(current_value.as_ref(), &when_value)?
                }
                ColumnarValue::Scalar(then_value) => {
                    zip(&when_value, &then_value.to_scalar()?, &current_value)?
                }
                ColumnarValue::Array(then_value) => {
                    zip(&when_value, &then_value, &current_value)?
                }
            };

            // Succeed tuples should be filtered out for short-circuit evaluation,
            // null values for the current when expr should be kept
            remainder = and_not(&remainder, &when_value)?;
        }

        if let Some(e) = &self.else_expr {
            // keep `else_expr`'s data type and return type consistent
            let expr = try_cast(Arc::clone(e), &batch.schema(), return_type.clone())
                .unwrap_or_else(|_| Arc::clone(e));
            let else_ = expr
                .evaluate_selection(batch, &remainder)?
                .into_array(batch.num_rows())?;
            current_value = zip(&remainder, &else_, &current_value)?;
        }

        Ok(ColumnarValue::Array(current_value))
    }
}

impl PhysicalExpr for CaseExpr {
    /// Return a reference to Any that can be used for down-casting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        // since all then results have the same data type, we can choose any one as the
        // return data type except for the null.
        let mut data_type = DataType::Null;
        for i in 0..self.when_then_expr.len() {
            data_type = self.when_then_expr[i].1.data_type(input_schema)?;
            if !data_type.equals_datatype(&DataType::Null) {
                break;
            }
        }
        // if all then results are null, we use data type of else expr instead if possible.
        if data_type.equals_datatype(&DataType::Null) {
            if let Some(e) = &self.else_expr {
                data_type = e.data_type(input_schema)?;
            }
        }

        Ok(data_type)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        // this expression is nullable if any of the input expressions are nullable
        let then_nullable = self
            .when_then_expr
            .iter()
            .map(|(_, t)| t.nullable(input_schema))
            .collect::<Result<Vec<_>>>()?;
        if then_nullable.contains(&true) {
            Ok(true)
        } else if let Some(e) = &self.else_expr {
            e.nullable(input_schema)
        } else {
            // CASE produces NULL if there is no `else` expr
            // (aka when none of the `when_then_exprs` match)
            Ok(true)
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.expr.is_some() {
            // this use case evaluates "expr" and then compares the values with the "when"
            // values
            self.case_when_with_expr(batch)
        } else {
            // The "when" conditions all evaluate to boolean in this use case and can be
            // arbitrary expressions
            self.case_when_no_expr(batch)
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        let mut children = vec![];
        if let Some(expr) = &self.expr {
            children.push(expr)
        }
        self.when_then_expr.iter().for_each(|(cond, value)| {
            children.push(cond);
            children.push(value);
        });

        if let Some(else_expr) = &self.else_expr {
            children.push(else_expr)
        }
        children
    }

    // For physical CaseExpr, we do not allow modifying children size
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if children.len() != self.children().len() {
            internal_err!("CaseExpr: Wrong number of children")
        } else {
            let (expr, when_then_expr, else_expr) =
                match (self.expr().is_some(), self.else_expr().is_some()) {
                    (true, true) => (
                        Some(&children[0]),
                        &children[1..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (true, false) => {
                        (Some(&children[0]), &children[1..children.len()], None)
                    }
                    (false, true) => (
                        None,
                        &children[0..children.len() - 1],
                        Some(&children[children.len() - 1]),
                    ),
                    (false, false) => (None, &children[0..children.len()], None),
                };
            Ok(Arc::new(CaseExpr::try_new(
                expr.cloned(),
                when_then_expr.iter().cloned().tuples().collect(),
                else_expr.cloned(),
            )?))
        }
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for CaseExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                let expr_eq = match (&self.expr, &x.expr) {
                    (Some(expr1), Some(expr2)) => expr1.eq(expr2),
                    (None, None) => true,
                    _ => false,
                };
                let else_expr_eq = match (&self.else_expr, &x.else_expr) {
                    (Some(expr1), Some(expr2)) => expr1.eq(expr2),
                    (None, None) => true,
                    _ => false,
                };
                expr_eq
                    && else_expr_eq
                    && self.when_then_expr.len() == x.when_then_expr.len()
                    && self.when_then_expr.iter().zip(x.when_then_expr.iter()).all(
                        |((when1, then1), (when2, then2))| {
                            when1.eq(when2) && then1.eq(then2)
                        },
                    )
            })
            .unwrap_or(false)
    }
}

/// Create a CASE expression
pub fn case(
    expr: Option<Arc<dyn PhysicalExpr>>,
    when_thens: Vec<WhenThen>,
    else_expr: Option<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(CaseExpr::try_new(expr, when_thens, else_expr)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{binary, cast, col, lit};

    use arrow::buffer::Buffer;
    use arrow::datatypes::DataType::Float64;
    use arrow::datatypes::*;
    use datafusion_common::cast::{as_float64_array, as_int32_array};
    use datafusion_common::plan_err;
    use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
    use datafusion_expr::type_coercion::binary::comparison_coercion;
    use datafusion_expr::Operator;

    #[test]
    fn case_with_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN 'foo' THEN 123 WHEN 'bar' THEN 456 ELSE 999 END
        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE a when 0 THEN float64(null) ELSE 25.0 / cast(a, float64)  END
        let when1 = lit(0i32);
        let then1 = lit(ScalarValue::Float64(None));
        let else_value = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected = &Int32Array::from(vec![Some(123), None, None, Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_expr_when_null() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE a WHEN NULL THEN 0 WHEN a THEN 123 ELSE 999 END
        let when1 = lit(ScalarValue::Utf8(None));
        let then1 = lit(0i32);
        let when2 = col("a", &schema)?;
        let then2 = lit(123i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(123), Some(999), Some(123)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_without_expr_divide_by_zero() -> Result<()> {
        let batch = case_test_batch1()?;
        let schema = batch.schema();

        // CASE WHEN a > 0 THEN 25.0 / cast(a, float64) ELSE float64(null) END
        let when1 = binary(col("a", &schema)?, Operator::Gt, lit(0i32), &batch.schema())?;
        let then1 = binary(
            lit(25.0f64),
            Operator::Divide,
            cast(col("a", &schema)?, &batch.schema(), Float64)?,
            &batch.schema(),
        )?;
        let x = lit(ScalarValue::Float64(None));

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1)],
            Some(x),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected = &Float64Array::from(vec![Some(25.0), None, None, Some(5.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch1() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let a = Int32Array::from(vec![Some(1), Some(0), None, Some(5)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }

    #[test]
    fn case_without_expr_else() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 999 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result = as_int32_array(&result)?;

        let expected =
            &Int32Array::from(vec![Some(123), Some(999), Some(999), Some(456)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_type_cast() -> Result<()> {
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123.3 ELSE 999 END
        let when = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then = lit(123.3f64);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            Some(else_value),
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(123.3), Some(999.0), Some(999.0), Some(999.0)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_with_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE WHEN load4 = 1.77 THEN load4 END
        let when = binary(
            col("load4", &schema)?,
            Operator::Eq,
            lit(1.77f64),
            &batch.schema(),
        )?;
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn case_expr_matches_and_nulls() -> Result<()> {
        let batch = case_test_batch_nulls()?;
        let schema = batch.schema();

        // SELECT CASE load4 WHEN 1.77 THEN load4 END
        let expr = col("load4", &schema)?;
        let when = lit(1.77f64);
        let then = col("load4", &schema)?;

        let expr = generate_case_when_with_type_coercion(
            Some(expr),
            vec![(when, then)],
            None,
            schema.as_ref(),
        )?;
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_float64_array(&result).expect("failed to downcast to Float64Array");

        let expected =
            &Float64Array::from(vec![Some(1.77), None, None, None, None, Some(1.77)]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn case_test_batch() -> Result<RecordBatch> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), Some("baz"), None, Some("bar")]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;
        Ok(batch)
    }

    // Construct an array that has several NULL values whose
    // underlying buffer actually matches the where expr predicate
    fn case_test_batch_nulls() -> Result<RecordBatch> {
        let load4: Float64Array = vec![
            Some(1.77), // 1.77
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.77), // null <-- same value, but will be set to null
            Some(1.78), // 1.78
            None,       // null
            Some(1.77), // 1.77
        ]
        .into_iter()
        .collect();

        //let valid_array = vec![true, false, false, true, false, tru
        let null_buffer = Buffer::from([0b00101001u8]);
        let load4 = load4
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()
            .unwrap();
        let load4: Float64Array = load4.into();

        let batch =
            RecordBatch::try_from_iter(vec![("load4", Arc::new(load4) as ArrayRef)])?;
        Ok(batch)
    }

    #[test]
    fn case_test_incompatible() -> Result<()> {
        // 1 then is int64
        // 2 then is boolean
        let batch = case_test_batch()?;
        let schema = batch.schema();

        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN true END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(true);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            None,
            schema.as_ref(),
        );
        assert!(expr.is_err());

        // then 1 is int32
        // then 2 is int64
        // else is float
        // CASE WHEN a = 'foo' THEN 123 WHEN a = 'bar' THEN 456 ELSE 1.23 END
        let when1 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("foo"),
            &batch.schema(),
        )?;
        let then1 = lit(123i32);
        let when2 = binary(
            col("a", &schema)?,
            Operator::Eq,
            lit("bar"),
            &batch.schema(),
        )?;
        let then2 = lit(456i64);
        let else_expr = lit(1.23f64);

        let expr = generate_case_when_with_type_coercion(
            None,
            vec![(when1, then1), (when2, then2)],
            Some(else_expr),
            schema.as_ref(),
        );
        assert!(expr.is_ok());
        let result_type = expr.unwrap().data_type(schema.as_ref())?;
        assert_eq!(DataType::Float64, result_type);
        Ok(())
    }

    #[test]
    fn case_eq() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr1 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr3 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(Arc::clone(&when1), Arc::clone(&then1)), (when2, then2)],
            None,
            &schema,
        )?;

        let expr4 = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![(when1, then1)],
            Some(else_value),
            &schema,
        )?;

        assert!(expr1.eq(&expr2));
        assert!(expr2.eq(&expr1));

        assert!(expr2.ne(&expr3));
        assert!(expr3.ne(&expr2));

        assert!(expr1.ne(&expr4));
        assert!(expr4.ne(&expr1));

        Ok(())
    }

    #[test]
    fn case_tranform() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

        let when1 = lit("foo");
        let then1 = lit(123i32);
        let when2 = lit("bar");
        let then2 = lit(456i32);
        let else_value = lit(999i32);

        let expr = generate_case_when_with_type_coercion(
            Some(col("a", &schema)?),
            vec![
                (Arc::clone(&when1), Arc::clone(&then1)),
                (Arc::clone(&when2), Arc::clone(&then2)),
            ],
            Some(Arc::clone(&else_value)),
            &schema,
        )?;

        let expr2 = Arc::clone(&expr)
            .transform(|e| {
                let transformed =
                    match e.as_any().downcast_ref::<crate::expressions::Literal>() {
                        Some(lit_value) => match lit_value.value() {
                            ScalarValue::Utf8(Some(str_value)) => {
                                Some(lit(str_value.to_uppercase()))
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        let expr3 = Arc::clone(&expr)
            .transform_down(|e| {
                let transformed =
                    match e.as_any().downcast_ref::<crate::expressions::Literal>() {
                        Some(lit_value) => match lit_value.value() {
                            ScalarValue::Utf8(Some(str_value)) => {
                                Some(lit(str_value.to_uppercase()))
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                Ok(if let Some(transformed) = transformed {
                    Transformed::yes(transformed)
                } else {
                    Transformed::no(e)
                })
            })
            .data()
            .unwrap();

        assert!(expr.ne(&expr2));
        assert!(expr2.eq(&expr3));

        Ok(())
    }

    fn generate_case_when_with_type_coercion(
        expr: Option<Arc<dyn PhysicalExpr>>,
        when_thens: Vec<WhenThen>,
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let coerce_type =
            get_case_common_type(&when_thens, else_expr.clone(), input_schema);
        let (when_thens, else_expr) = match coerce_type {
            None => plan_err!(
                "Can't get a common type for then {when_thens:?} and else {else_expr:?} expression"
            ),
            Some(data_type) => {
                // cast then expr
                let left = when_thens
                    .into_iter()
                    .map(|(when, then)| {
                        let then = try_cast(then, input_schema, data_type.clone())?;
                        Ok((when, then))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let right = match else_expr {
                    None => None,
                    Some(expr) => Some(try_cast(expr, input_schema, data_type.clone())?),
                };

                Ok((left, right))
            }
        }?;
        case(expr, when_thens, else_expr)
    }

    fn get_case_common_type(
        when_thens: &[WhenThen],
        else_expr: Option<Arc<dyn PhysicalExpr>>,
        input_schema: &Schema,
    ) -> Option<DataType> {
        let thens_type = when_thens
            .iter()
            .map(|when_then| {
                let data_type = &when_then.1.data_type(input_schema).unwrap();
                data_type.clone()
            })
            .collect::<Vec<_>>();
        let else_type = match else_expr {
            None => {
                // case when then exprs must have one then value
                thens_type[0].clone()
            }
            Some(else_phy_expr) => else_phy_expr.data_type(input_schema).unwrap(),
        };
        thens_type
            .iter()
            .try_fold(else_type, |left_type, right_type| {
                // TODO: now just use the `equal` coercion rule for case when. If find the issue, and
                // refactor again.
                comparison_coercion(&left_type, right_type)
            })
    }
}
