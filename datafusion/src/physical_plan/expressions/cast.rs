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

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use super::ColumnarValue;
use crate::error::{DataFusionError, Result};
use crate::physical_plan::PhysicalExpr;
use crate::scalar::ScalarValue;
use arrow::compute;
use arrow::compute::kernels;
use arrow::compute::CastOptions;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use compute::can_cast_types;

/// provide Datafusion default cast options
pub const DEFAULT_DATAFUSION_CAST_OPTIONS: CastOptions = CastOptions { safe: false };

/// CAST expression casts an expression to a specific data type and returns a runtime error on invalid cast
#[derive(Debug)]
pub struct CastExpr {
    /// The expression to cast
    expr: Arc<dyn PhysicalExpr>,
    /// The data type to cast to
    cast_type: DataType,
    /// Cast options
    cast_options: CastOptions,
}

impl CastExpr {
    /// Create a new CastExpr
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        cast_type: DataType,
        cast_options: CastOptions,
    ) -> Self {
        Self {
            expr,
            cast_type,
            cast_options,
        }
    }

    /// The expression to cast
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// The data type to cast to
    pub fn cast_type(&self) -> &DataType {
        &self.cast_type
    }
}

impl fmt::Display for CastExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST({} AS {:?})", self.expr, self.cast_type)
    }
}

impl PhysicalExpr for CastExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        cast_column(&value, &self.cast_type, &self.cast_options)
    }
}

/// Internal cast function for casting ColumnarValue -> ColumnarValue for cast_type
pub fn cast_column(
    value: &ColumnarValue,
    cast_type: &DataType,
    cast_options: &CastOptions,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
            kernels::cast::cast_with_options(array, cast_type, cast_options)?,
        )),
        ColumnarValue::Scalar(scalar) => {
            let scalar_array = scalar.to_array();
            let cast_array =
                kernels::cast::cast_with_options(&scalar_array, cast_type, cast_options)?;
            let cast_scalar = ScalarValue::try_from_array(&cast_array, 0)?;
            Ok(ColumnarValue::Scalar(cast_scalar))
        }
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast_with_options(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
    cast_options: CastOptions,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr_type = expr.data_type(input_schema)?;
    if expr_type == cast_type {
        Ok(expr.clone())
    } else if can_cast_types(&expr_type, &cast_type) {
        Ok(Arc::new(CastExpr::new(expr, cast_type, cast_options)))
    } else {
        Err(DataFusionError::Internal(format!(
            "Unsupported CAST from {:?} to {:?}",
            expr_type, cast_type
        )))
    }
}

/// Return a PhysicalExpression representing `expr` casted to
/// `cast_type`, if any casting is needed.
///
/// Note that such casts may lose type information
pub fn cast(
    expr: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
    cast_type: DataType,
) -> Result<Arc<dyn PhysicalExpr>> {
    cast_with_options(
        expr,
        input_schema,
        cast_type,
        DEFAULT_DATAFUSION_CAST_OPTIONS,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::from_slice::FromSlice;
    use crate::physical_plan::expressions::col;
    use arrow::{
        array::{
            Array, DecimalArray, DecimalBuilder, Float32Array, Float64Array, Int16Array,
            Int32Array, Int64Array, Int8Array, StringArray, Time64NanosecondArray,
            TimestampNanosecondArray, UInt32Array,
        },
        datatypes::*,
    };

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_decimal_to_other_test_cast {
        ($DECIMAL_ARRAY:ident, $A_TYPE:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr,$CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, false)]);
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new($DECIMAL_ARRAY)],
            )?;
            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(
                format!("CAST(a@0 AS {:?})", $TYPE),
                format!("{}", expression)
            );

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(!result.is_valid(i)),
                }
            }
        }};
    }

    // runs an end-to-end test of physical type cast
    // 1. construct a record batch with a column "a" of type A
    // 2. construct a physical expression of CAST(a AS B)
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type B
    // 5. verify that the resulting values are downcastable and correct
    macro_rules! generic_test_cast {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $TYPEARRAY:ident, $TYPE:expr, $VEC:expr, $CAST_OPTIONS:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $A_TYPE, false)]);
            let a = $A_ARRAY::from($A_VEC);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

            // verify that we can construct the expression
            let expression =
                cast_with_options(col("a", &schema)?, &schema, $TYPE, $CAST_OPTIONS)?;

            // verify that its display is correct
            assert_eq!(
                format!("CAST(a@0 AS {:?})", $TYPE),
                format!("{}", expression)
            );

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $TYPE);

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $TYPE);

            // verify that the len is correct
            assert_eq!(result.len(), $A_VEC.len());

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$TYPEARRAY>()
                .expect("failed to downcast");

            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                match x {
                    Some(x) => assert_eq!(result.value(i), *x),
                    None => assert!(!result.is_valid(i)),
                }
            }
        }};
    }

    fn create_decimal_array(
        array: &[i128],
        precision: usize,
        scale: usize,
    ) -> Result<DecimalArray> {
        let mut decimal_builder = DecimalBuilder::new(array.len(), precision, scale);
        for value in array {
            decimal_builder.append_value(*value)?
        }
        decimal_builder.append_null()?;
        Ok(decimal_builder.finish())
    }

    #[test]
    fn test_cast_decimal_to_decimal() -> Result<()> {
        let array: Vec<i128> = vec![1234, 2222, 3, 4000, 5000];
        let decimal_array = create_decimal_array(&array, 10, 3)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 3),
            DecimalArray,
            DataType::Decimal(20, 6),
            vec![
                Some(1_234_000_i128),
                Some(2_222_000_i128),
                Some(3_000_i128),
                Some(4_000_000_i128),
                Some(5_000_000_i128),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        let decimal_array = create_decimal_array(&array, 10, 3)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 3),
            DecimalArray,
            DataType::Decimal(10, 2),
            vec![
                Some(123_i128),
                Some(222_i128),
                Some(0_i128),
                Some(400_i128),
                Some(500_i128),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        Ok(())
    }

    #[test]
    fn test_cast_decimal_to_numeric() -> Result<()> {
        let array: Vec<i128> = vec![1, 2, 3, 4, 5];
        // decimal to i8
        let decimal_array = create_decimal_array(&array, 10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 0),
            Int8Array,
            DataType::Int8,
            vec![
                Some(1_i8),
                Some(2_i8),
                Some(3_i8),
                Some(4_i8),
                Some(5_i8),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        // decimal to i16
        let decimal_array = create_decimal_array(&array, 10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 0),
            Int16Array,
            DataType::Int16,
            vec![
                Some(1_i16),
                Some(2_i16),
                Some(3_i16),
                Some(4_i16),
                Some(5_i16),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        // decimal to i32
        let decimal_array = create_decimal_array(&array, 10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 0),
            Int32Array,
            DataType::Int32,
            vec![
                Some(1_i32),
                Some(2_i32),
                Some(3_i32),
                Some(4_i32),
                Some(5_i32),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        // decimal to i64
        let decimal_array = create_decimal_array(&array, 10, 0)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 0),
            Int64Array,
            DataType::Int64,
            vec![
                Some(1_i64),
                Some(2_i64),
                Some(3_i64),
                Some(4_i64),
                Some(5_i64),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        // decimal to float32
        let array: Vec<i128> = vec![1234, 2222, 3, 4000, 5000];
        let decimal_array = create_decimal_array(&array, 10, 3)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(10, 3),
            Float32Array,
            DataType::Float32,
            vec![
                Some(1.234_f32),
                Some(2.222_f32),
                Some(0.003_f32),
                Some(4.0_f32),
                Some(5.0_f32),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        // decimal to float64
        let decimal_array = create_decimal_array(&array, 20, 6)?;
        generic_decimal_to_other_test_cast!(
            decimal_array,
            DataType::Decimal(20, 6),
            Float64Array,
            DataType::Float64,
            vec![
                Some(0.001234_f64),
                Some(0.002222_f64),
                Some(0.000003_f64),
                Some(0.004_f64),
                Some(0.005_f64),
                None,
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        Ok(())
    }

    #[test]
    fn test_cast_numeric_to_decimal() -> Result<()> {
        // int8
        generic_test_cast!(
            Int8Array,
            DataType::Int8,
            vec![1, 2, 3, 4, 5],
            DecimalArray,
            DataType::Decimal(3, 0),
            vec![
                Some(1_i128),
                Some(2_i128),
                Some(3_i128),
                Some(4_i128),
                Some(5_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // int16
        generic_test_cast!(
            Int16Array,
            DataType::Int16,
            vec![1, 2, 3, 4, 5],
            DecimalArray,
            DataType::Decimal(5, 0),
            vec![
                Some(1_i128),
                Some(2_i128),
                Some(3_i128),
                Some(4_i128),
                Some(5_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // int32
        generic_test_cast!(
            Int32Array,
            DataType::Int32,
            vec![1, 2, 3, 4, 5],
            DecimalArray,
            DataType::Decimal(10, 0),
            vec![
                Some(1_i128),
                Some(2_i128),
                Some(3_i128),
                Some(4_i128),
                Some(5_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // int64
        generic_test_cast!(
            Int64Array,
            DataType::Int64,
            vec![1, 2, 3, 4, 5],
            DecimalArray,
            DataType::Decimal(20, 0),
            vec![
                Some(1_i128),
                Some(2_i128),
                Some(3_i128),
                Some(4_i128),
                Some(5_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // int64 to different scale
        generic_test_cast!(
            Int64Array,
            DataType::Int64,
            vec![1, 2, 3, 4, 5],
            DecimalArray,
            DataType::Decimal(20, 2),
            vec![
                Some(100_i128),
                Some(200_i128),
                Some(300_i128),
                Some(400_i128),
                Some(500_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // float32
        generic_test_cast!(
            Float32Array,
            DataType::Float32,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            DecimalArray,
            DataType::Decimal(10, 2),
            vec![
                Some(150_i128),
                Some(250_i128),
                Some(300_i128),
                Some(112_i128),
                Some(550_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );

        // float64
        generic_test_cast!(
            Float64Array,
            DataType::Float64,
            vec![1.5, 2.5, 3.0, 1.123_456_8, 5.50],
            DecimalArray,
            DataType::Decimal(20, 4),
            vec![
                Some(15000_i128),
                Some(25000_i128),
                Some(30000_i128),
                Some(11234_i128),
                Some(55000_i128),
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_u32() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            DataType::Int32,
            vec![1, 2, 3, 4, 5],
            UInt32Array,
            DataType::UInt32,
            vec![
                Some(1_u32),
                Some(2_u32),
                Some(3_u32),
                Some(4_u32),
                Some(5_u32)
            ],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        Ok(())
    }

    #[test]
    fn test_cast_i32_utf8() -> Result<()> {
        generic_test_cast!(
            Int32Array,
            DataType::Int32,
            vec![1, 2, 3, 4, 5],
            StringArray,
            DataType::Utf8,
            vec![Some("1"), Some("2"), Some("3"), Some("4"), Some("5")],
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        Ok(())
    }

    #[allow(clippy::redundant_clone)]
    #[test]
    fn test_cast_i64_t64() -> Result<()> {
        let original = vec![1, 2, 3, 4, 5];
        let expected: Vec<Option<i64>> = original
            .iter()
            .map(|i| Some(Time64NanosecondArray::from(vec![*i]).value(0)))
            .collect();
        generic_test_cast!(
            Int64Array,
            DataType::Int64,
            original.clone(),
            TimestampNanosecondArray,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            expected,
            DEFAULT_DATAFUSION_CAST_OPTIONS
        );
        Ok(())
    }

    #[test]
    fn invalid_cast() {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let result = cast(col("a", &schema).unwrap(), &schema, DataType::LargeBinary);
        result.expect_err("expected Invalid CAST");
    }

    #[test]
    fn invalid_cast_with_options_error() -> Result<()> {
        // Ensure a useful error happens at plan time if invalid casts are used
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, false)]);
        let a = StringArray::from_slice(&["9.1"]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;
        let expression = cast_with_options(
            col("a", &schema)?,
            &schema,
            DataType::Int32,
            DEFAULT_DATAFUSION_CAST_OPTIONS,
        )?;
        let result = expression.evaluate(&batch);

        match result {
            Ok(_) => panic!("expected error"),
            Err(e) => {
                assert!(e.to_string().contains(
                    "Cast error: Cannot cast string '9.1' to value of arrow::datatypes::types::Int32Type type"
                ))
            }
        }
        Ok(())
    }
}
