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

//! Any expression

use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    BooleanArray, Int16Array, Int32Array, Int64Array, Int8Array, ListArray,
    PrimitiveArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::expressions::try_cast;
use crate::PhysicalExpr;
use arrow::array::*;

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{ColumnarValue, Operator};

macro_rules! compare_op_scalar {
    ($LEFT: expr, $LIST_VALUES:expr, $OP:expr, $LIST_VALUES_TYPE:ty, $LIST_FROM_SCALAR: expr) => {{
        let mut builder = BooleanBuilder::new($LEFT.len());

        if $LIST_FROM_SCALAR {
            for i in 0..$LEFT.len() {
                if $LEFT.is_null(i) {
                    builder.append_null()?;
                } else {
                    if $LIST_VALUES.is_null(0) {
                        builder.append_null()?;
                    } else {
                        builder.append_value($OP(
                            $LEFT.value(i),
                            $LIST_VALUES
                                .value(0)
                                .as_any()
                                .downcast_ref::<$LIST_VALUES_TYPE>()
                                .unwrap(),
                        ))?;
                    }
                }
            }
        } else {
            for i in 0..$LEFT.len() {
                if $LEFT.is_null(i) {
                    builder.append_null()?;
                } else {
                    if $LIST_VALUES.is_null(i) {
                        builder.append_null()?;
                    } else {
                        builder.append_value($OP(
                            $LEFT.value(i),
                            $LIST_VALUES
                                .value(i)
                                .as_any()
                                .downcast_ref::<$LIST_VALUES_TYPE>()
                                .unwrap(),
                        ))?;
                    }
                }
            }
        }

        Ok(builder.finish())
    }};
}

macro_rules! make_primitive {
    ($VALUES:expr, $IN_VALUES:expr, $NEGATED:expr, $TYPE:ident, $LIST_FROM_SCALAR: expr) => {{
        let left = $VALUES.as_any().downcast_ref::<$TYPE>().expect(&format!(
            "Unable to downcast values to {}",
            stringify!($TYPE)
        ));

        if $NEGATED {
            Ok(ColumnarValue::Array(Arc::new(neq_primitive(
                left,
                $IN_VALUES,
                $LIST_FROM_SCALAR,
            )?)))
        } else {
            Ok(ColumnarValue::Array(Arc::new(eq_primitive(
                left,
                $IN_VALUES,
                $LIST_FROM_SCALAR,
            )?)))
        }
    }};
}

fn eq_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &PrimitiveArray<T>| v.values().contains(&x),
        PrimitiveArray<T>,
        list_from_scalar
    )
}

fn neq_primitive<T: ArrowPrimitiveType>(
    array: &PrimitiveArray<T>,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &PrimitiveArray<T>| !v.values().contains(&x),
        PrimitiveArray<T>,
        list_from_scalar
    )
}

fn eq_bool(
    array: &BooleanArray,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &BooleanArray| unsafe {
            for i in 0..v.len() {
                if v.value_unchecked(i) == x {
                    return true;
                }
            }

            false
        },
        BooleanArray,
        list_from_scalar
    )
}

fn neq_bool(
    array: &BooleanArray,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &BooleanArray| unsafe {
            for i in 0..v.len() {
                if v.value_unchecked(i) == x {
                    return false;
                }
            }

            true
        },
        BooleanArray,
        list_from_scalar
    )
}

fn eq_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &GenericStringArray<OffsetSize>| unsafe {
            for i in 0..v.len() {
                if v.value_unchecked(i) == x {
                    return true;
                }
            }

            false
        },
        GenericStringArray<OffsetSize>,
        list_from_scalar
    )
}

fn neq_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    list: &ListArray,
    list_from_scalar: bool,
) -> Result<BooleanArray> {
    compare_op_scalar!(
        array,
        list,
        |x, v: &GenericStringArray<OffsetSize>| unsafe {
            for i in 0..v.len() {
                if v.value_unchecked(i) == x {
                    return false;
                }
            }

            true
        },
        GenericStringArray<OffsetSize>,
        list_from_scalar
    )
}

/// AnyExpr
#[derive(Debug)]
pub struct AnyExpr {
    value: Arc<dyn PhysicalExpr>,
    op: Operator,
    list: Arc<dyn PhysicalExpr>,
}

impl AnyExpr {
    /// Create a new Any expression
    pub fn new(
        value: Arc<dyn PhysicalExpr>,
        op: Operator,
        list: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { value, op, list }
    }

    /// Compare for specific utf8 types
    fn compare_utf8<T: OffsetSizeTrait>(
        &self,
        array: ArrayRef,
        list: &ListArray,
        negated: bool,
        list_from_scalar: bool,
    ) -> Result<ColumnarValue> {
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .unwrap();

        if negated {
            Ok(ColumnarValue::Array(Arc::new(neq_utf8(
                array,
                list,
                list_from_scalar,
            )?)))
        } else {
            Ok(ColumnarValue::Array(Arc::new(eq_utf8(
                array,
                list,
                list_from_scalar,
            )?)))
        }
    }

    /// Get the left side of the binary expression
    pub fn left(&self) -> &Arc<dyn PhysicalExpr> {
        &self.value
    }

    /// Get the right side of the binary expression
    pub fn right(&self) -> &Arc<dyn PhysicalExpr> {
        &self.list
    }

    /// Get the operator for this binary expression
    pub fn op(&self) -> &Operator {
        &self.op
    }

    /// Compare for specific utf8 types
    fn compare_bool(
        &self,
        array: ArrayRef,
        list: &ListArray,
        negated: bool,
        list_from_scalar: bool,
    ) -> Result<ColumnarValue> {
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();

        if negated {
            Ok(ColumnarValue::Array(Arc::new(neq_bool(
                array,
                list,
                list_from_scalar,
            )?)))
        } else {
            Ok(ColumnarValue::Array(Arc::new(eq_bool(
                array,
                list,
                list_from_scalar,
            )?)))
        }
    }
}

impl std::fmt::Display for AnyExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} ANY({})", self.value, self.op, self.list)
    }
}

impl PhysicalExpr for AnyExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.value.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = match self.value.evaluate(batch)? {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array(),
        };

        let (list, list_from_scalar) = match self.list.evaluate(batch)? {
            ColumnarValue::Array(array) => (array, false),
            ColumnarValue::Scalar(scalar) => (scalar.to_array(), true),
        };
        let as_list = list
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Unable to downcast list to ListArray");

        let negated = match self.op {
            Operator::Eq => false,
            Operator::NotEq => true,
            op => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Operator for ANY expression, actual: {:?}",
                    op
                )));
            }
        };

        match value.data_type() {
            DataType::Float16 => {
                make_primitive!(value, as_list, negated, Float16Array, list_from_scalar)
            }
            DataType::Float32 => {
                make_primitive!(value, as_list, negated, Float32Array, list_from_scalar)
            }
            DataType::Float64 => {
                make_primitive!(value, as_list, negated, Float64Array, list_from_scalar)
            }
            DataType::Int8 => {
                make_primitive!(value, as_list, negated, Int8Array, list_from_scalar)
            }
            DataType::Int16 => {
                make_primitive!(value, as_list, negated, Int16Array, list_from_scalar)
            }
            DataType::Int32 => {
                make_primitive!(value, as_list, negated, Int32Array, list_from_scalar)
            }
            DataType::Int64 => {
                make_primitive!(value, as_list, negated, Int64Array, list_from_scalar)
            }
            DataType::UInt8 => {
                make_primitive!(value, as_list, negated, UInt8Array, list_from_scalar)
            }
            DataType::UInt16 => {
                make_primitive!(value, as_list, negated, UInt16Array, list_from_scalar)
            }
            DataType::UInt32 => {
                make_primitive!(value, as_list, negated, UInt32Array, list_from_scalar)
            }
            DataType::UInt64 => {
                make_primitive!(value, as_list, negated, UInt64Array, list_from_scalar)
            }
            DataType::Boolean => {
                self.compare_bool(value, as_list, negated, list_from_scalar)
            }
            DataType::Utf8 => {
                self.compare_utf8::<i32>(value, as_list, negated, list_from_scalar)
            }
            DataType::LargeUtf8 => {
                self.compare_utf8::<i64>(value, as_list, negated, list_from_scalar)
            }
            datatype => Result::Err(DataFusionError::NotImplemented(format!(
                "AnyExpr does not support datatype {:?}.",
                datatype
            ))),
        }
    }
}

/// return two physical expressions that are optionally coerced to a
/// common type that the binary operator supports.
fn any_cast(
    value: Arc<dyn PhysicalExpr>,
    _op: &Operator,
    list: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let list_dt = list.data_type(input_schema)?;
    let list_type = match &list_dt {
        DataType::List(f) => f.data_type(),
        dt => return Err(DataFusionError::Execution(format!(
            "Unexpected type on the right side of ANY expression. Must be a List, actual: {}",
            dt
        ))),
    };

    Ok((try_cast(value, input_schema, list_type.clone())?, list))
}

/// Creates an expression AnyExpr
pub fn any(
    value: Arc<dyn PhysicalExpr>,
    op: Operator,
    list: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (l, r) = any_cast(value, &op, list, input_schema)?;
    Ok(Arc::new(AnyExpr::new(l, op, r)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;
    use crate::expressions::{col, lit};
    use datafusion_common::{Result, ScalarValue};

    // applies the any expr to an input batch
    macro_rules! execute_any {
        ($BATCH:expr, $OP:expr, $EXPECTED:expr, $COL_A:expr, $COL_B:expr, $SCHEMA:expr) => {{
            let expr = any($COL_A, $OP, $COL_B, $SCHEMA).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn any_int64_array_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, true);
        let field_b = Field::new(
            "b",
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            true,
        );

        let schema = Schema::new(vec![field_a, field_b]);
        let a = Int64Array::from(vec![Some(0), Some(3), None]);
        let col_a = col("a", &schema)?;

        let values_builder = Int64Builder::new(3 * 3);
        let mut builder = ListBuilder::new(values_builder);

        for _ in 0..3 {
            builder.values().append_value(0).unwrap();
            builder.values().append_value(1).unwrap();
            builder.values().append_value(2).unwrap();
            builder.append(true).unwrap();
        }

        let b = builder.finish();
        let col_b = col("b", &schema)?;

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )?;

        execute_any!(
            batch,
            Operator::Eq,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            col_b.clone(),
            &schema
        );

        Ok(())
    }

    /// This tests emulates a simple type coercion, where the right side is the main type of expression
    #[test]
    fn any_int32_column_int64_array_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Int32, true);
        let field_b = Field::new(
            "b",
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            true,
        );

        let schema = Schema::new(vec![field_a, field_b]);
        let a = Int32Array::from(vec![Some(0), Some(3), None]);
        let col_a = col("a", &schema)?;

        let values_builder = Int64Builder::new(3 * 3);
        let mut builder = ListBuilder::new(values_builder);

        for _ in 0..3 {
            builder.values().append_value(0).unwrap();
            builder.values().append_value(1).unwrap();
            builder.values().append_value(2).unwrap();
            builder.append(true).unwrap();
        }

        let b = builder.finish();
        let col_b = col("b", &schema)?;

        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(a), Arc::new(b)],
        )?;

        execute_any!(
            batch,
            Operator::Eq,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            col_b.clone(),
            &schema
        );

        Ok(())
    }

    // applies the any expr to an input batch and list (scalar)
    macro_rules! execute_any_scalar {
        ($BATCH:expr, $LIST:expr, $OP:expr, $EXPECTED:expr, $COL:expr, $SCHEMA:expr) => {{
            let expr = any($COL, $OP, $LIST, $SCHEMA).unwrap();
            let result = expr.evaluate(&$BATCH)?.into_array($BATCH.num_rows());
            let result = result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("failed to downcast to BooleanArray");
            let expected = &BooleanArray::from($EXPECTED);
            assert_eq!(expected, result);
        }};
    }

    #[test]
    fn any_int64_scalar_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, true);
        let schema = Schema::new(vec![field_a.clone()]);
        let a = Int64Array::from(vec![Some(0), Some(3), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a = ANY (0, 1, 2)"
        let list = lit(ScalarValue::List(
            Some(vec![
                ScalarValue::Int64(Some(0)),
                ScalarValue::Int64(Some(1)),
                ScalarValue::Int64(Some(2)),
            ]),
            Box::new(DataType::Int64),
        ));

        let schema = &Schema::new(vec![
            field_a,
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("d", DataType::Int64, true))),
                true,
            ),
        ]);
        execute_any_scalar!(
            batch,
            list,
            Operator::Eq,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            schema
        );

        Ok(())
    }

    #[test]
    fn any_utf8_scalar_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Utf8, true);
        let schema = Schema::new(vec![field_a.clone()]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a = ANY ('a', 'b', 'c')"
        let list = lit(ScalarValue::List(
            Some(vec![
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("b".to_string())),
                ScalarValue::Utf8(Some("c".to_string())),
            ]),
            Box::new(DataType::Utf8),
        ));

        let schema = &Schema::new(vec![
            field_a,
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("d", DataType::Utf8, true))),
                true,
            ),
        ]);
        execute_any_scalar!(
            batch,
            list,
            Operator::Eq,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            schema
        );

        Ok(())
    }

    #[test]
    fn any_bool_scalar_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, true);
        let schema = Schema::new(vec![field_a.clone()]);
        let a = BooleanArray::from(vec![Some(true), Some(false), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        // expression: "a = ANY (true)"
        let list = lit(ScalarValue::List(
            Some(vec![ScalarValue::Boolean(Some(true))]),
            Box::new(DataType::Boolean),
        ));

        let schema = &Schema::new(vec![
            field_a,
            Field::new(
                "b",
                DataType::List(Box::new(Field::new("d", DataType::Boolean, true))),
                true,
            ),
        ]);
        execute_any_scalar!(
            batch,
            list,
            Operator::Eq,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            schema
        );

        Ok(())
    }
}
