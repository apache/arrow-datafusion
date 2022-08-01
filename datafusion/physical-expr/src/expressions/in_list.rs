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

//! InList expression

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::GenericStringArray;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, OffsetSizeTrait, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::PhysicalExpr;
use arrow::array::*;
use arrow::buffer::{Buffer, MutableBuffer};
use datafusion_common::ScalarValue;
use datafusion_common::ScalarValue::{
    Boolean, Decimal128, Int16, Int32, Int64, Int8, LargeUtf8, UInt16, UInt32, UInt64,
    UInt8, Utf8,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

/// Size at which to use a Set rather than Vec for `IN` / `NOT IN`
/// Value chosen by the benchmark at
/// https://github.com/apache/arrow-datafusion/pull/2156#discussion_r845198369
/// TODO: add switch codeGen in In_List
static OPTIMIZER_INSET_THRESHOLD: usize = 30;

macro_rules! compare_op_scalar {
    ($left: expr, $right:expr, $op:expr) => {{
        let null_bit_buffer = $left.data().null_buffer().cloned();

        let comparison =
            (0..$left.len()).map(|i| unsafe { $op($left.value_unchecked(i), $right) });
        // same as $left.len()
        let buffer = unsafe { MutableBuffer::from_trusted_len_iter_bool(comparison) };

        let data = unsafe {
            ArrayData::new_unchecked(
                DataType::Boolean,
                $left.len(),
                None,
                null_bit_buffer,
                0,
                vec![Buffer::from(buffer)],
                vec![],
            )
        };
        Ok(BooleanArray::from(data))
    }};
}

/// InList
#[derive(Debug)]
pub struct InListExpr {
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: bool,
    set: Option<InSet>,
}

/// InSet
#[derive(Debug)]
pub struct InSet {
    // TODO: optimization: In the `IN` or `NOT IN` we don't need to consider the NULL value
    // The data type is same, we can use  set: HashSet<T>
    set: HashSet<ScalarValue>,
}

impl InSet {
    pub fn new(set: HashSet<ScalarValue>) -> Self {
        Self { set }
    }

    pub fn get_set(&self) -> &HashSet<ScalarValue> {
        &self.set
    }
}

macro_rules! make_contains {
    ($ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $ARRAY_TYPE:ident) => {{
        let array = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

        let contains_null = $LIST_VALUES
            .iter()
            .any(|v| matches!(v, ColumnarValue::Scalar(s) if s.is_null()));
        let values = $LIST_VALUES
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::$SCALAR_VALUE(Some(v)) => Some(*v),
                    ScalarValue::$SCALAR_VALUE(None) => None,
                    datatype => unreachable!("InList can't reach other data type {} for {}.", datatype, s),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<_>>();

        collection_contains_check!(array, values, $NEGATED, contains_null)
    }};
}

macro_rules! make_contains_primitive {
    ($ARRAY:expr, $LIST_VALUES:expr, $NEGATED:expr, $SCALAR_VALUE:ident, $ARRAY_TYPE:ident) => {{
        let array = $ARRAY.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();

        let contains_null = $LIST_VALUES
            .iter()
            .any(|v| matches!(v, ColumnarValue::Scalar(s) if s.is_null()));
        let values = $LIST_VALUES
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::$SCALAR_VALUE(Some(v)) => Some(*v),
                    ScalarValue::$SCALAR_VALUE(None) => None,
                    datatype => unreachable!("InList can't reach other data type {} for {}.", datatype, s),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<_>>();

        Ok(collection_contains_check!(array, values, $NEGATED, contains_null))
    }};
}

macro_rules! set_contains_for_float {
    ($ARRAY:expr, $SET_VALUES:expr, $SCALAR_VALUE:ident, $NEGATED:expr, $PHY_TYPE:ty) => {{
        let contains_null = $SET_VALUES.iter().any(|s| s.is_null());
        let bool_array = if $NEGATED {
            // Not in
            if contains_null {
                $ARRAY
                    .iter()
                    .map(|vop| {
                        match vop.map(|v| !$SET_VALUES.contains(&v.try_into().unwrap())) {
                            Some(true) => None,
                            x => x,
                        }
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| !$SET_VALUES.contains(&v.try_into().unwrap())))
                    .collect::<BooleanArray>()
            }
        } else {
            // In
            if contains_null {
                $ARRAY
                    .iter()
                    .map(|vop| {
                        match vop.map(|v| $SET_VALUES.contains(&v.try_into().unwrap())) {
                            Some(false) => None,
                            x => x,
                        }
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| $SET_VALUES.contains(&v.try_into().unwrap())))
                    .collect::<BooleanArray>()
            }
        };
        ColumnarValue::Array(Arc::new(bool_array))
    }};
}

macro_rules! set_contains_for_primitive {
    ($ARRAY:expr, $SET_VALUES:expr, $SCALAR_VALUE:ident, $NEGATED:expr, $PHY_TYPE:ty) => {{
        let contains_null = $SET_VALUES.iter().any(|s| s.is_null());
        let native_array = $SET_VALUES
            .iter()
            .flat_map(|v| match v {
                $SCALAR_VALUE(value) => *value,
                datatype => {
                    unreachable!(
                        "InList can't reach other data type {} for {}.",
                        datatype, v
                    )
                }
            })
            .collect::<Vec<_>>();
        let native_set: HashSet<$PHY_TYPE> = HashSet::from_iter(native_array);

        collection_contains_check!($ARRAY, native_set, $NEGATED, contains_null)
    }};
}

macro_rules! collection_contains_check {
    ($ARRAY:expr, $VALUES:expr, $NEGATED:expr, $CONTAINS_NULL:expr) => {{
        let bool_array = if $NEGATED {
            // Not in
            if $CONTAINS_NULL {
                $ARRAY
                    .iter()
                    .map(|vop| match vop.map(|v| !$VALUES.contains(&v)) {
                        Some(true) => None,
                        x => x,
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| !$VALUES.contains(&v)))
                    .collect::<BooleanArray>()
            }
        } else {
            // In
            if $CONTAINS_NULL {
                $ARRAY
                    .iter()
                    .map(|vop| match vop.map(|v| $VALUES.contains(&v)) {
                        Some(false) => None,
                        x => x,
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| $VALUES.contains(&v)))
                    .collect::<BooleanArray>()
            }
        };
        ColumnarValue::Array(Arc::new(bool_array))
    }};
}

macro_rules! collection_contains_check_decimal {
    ($ARRAY:expr, $VALUES:expr, $NEGATED:expr, $CONTAINS_NULL:expr) => {{
        let bool_array = if $NEGATED {
            // Not in
            if $CONTAINS_NULL {
                $ARRAY
                    .iter()
                    .map(|vop| match vop.map(|v| !$VALUES.contains(&v.as_i128())) {
                        Some(true) => None,
                        x => x,
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| !$VALUES.contains(&v.as_i128())))
                    .collect::<BooleanArray>()
            }
        } else {
            // In
            if $CONTAINS_NULL {
                $ARRAY
                    .iter()
                    .map(|vop| match vop.map(|v| $VALUES.contains(&v.as_i128())) {
                        Some(false) => None,
                        x => x,
                    })
                    .collect::<BooleanArray>()
            } else {
                $ARRAY
                    .iter()
                    .map(|vop| vop.map(|v| $VALUES.contains(&v.as_i128())))
                    .collect::<BooleanArray>()
            }
        };
        ColumnarValue::Array(Arc::new(bool_array))
    }};
}

// whether each value on the left (can be null) is contained in the non-null list
fn in_list_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    values: &[&str],
) -> Result<BooleanArray> {
    compare_op_scalar!(array, values, |x, v: &[&str]| v.contains(&x))
}

fn not_in_list_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    values: &[&str],
) -> Result<BooleanArray> {
    compare_op_scalar!(array, values, |x, v: &[&str]| !v.contains(&x))
}

// try evaluate all list exprs and check if the exprs are constants or not
fn try_cast_static_filter_to_set(
    list: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> Result<HashSet<ScalarValue>> {
    let batch = RecordBatch::new_empty(Arc::new(schema.to_owned()));
    match list
        .iter()
        .map(|expr| match expr.evaluate(&batch) {
            Ok(ColumnarValue::Array(_)) => Err(DataFusionError::NotImplemented(
                "InList doesn't support to evaluate the array result".to_string(),
            )),
            Ok(ColumnarValue::Scalar(s)) => Ok(s),
            Err(e) => Err(e),
        })
        .collect::<Result<Vec<_>>>()
    {
        Ok(s) => Ok(HashSet::from_iter(s)),
        Err(e) => Err(e),
    }
}

fn make_list_contains_decimal(
    array: &Decimal128Array,
    list: Vec<ColumnarValue>,
    negated: bool,
) -> ColumnarValue {
    let contains_null = list
        .iter()
        .any(|v| matches!(v, ColumnarValue::Scalar(s) if s.is_null()));
    let values = list
        .iter()
        .flat_map(|v| match v {
            ColumnarValue::Scalar(s) => match s {
                Decimal128(v128op, _, _) => *v128op,
                datatype => unreachable!(
                    "InList can't reach other data type {} for {}.",
                    datatype, s
                ),
            },
            ColumnarValue::Array(_) => {
                unimplemented!("InList does not yet support nested columns.")
            }
        })
        .collect::<Vec<_>>();

    collection_contains_check_decimal!(array, values, negated, contains_null)
}

fn make_set_contains_decimal(
    array: &Decimal128Array,
    set: &HashSet<ScalarValue>,
    negated: bool,
) -> ColumnarValue {
    let contains_null = set.iter().any(|v| v.is_null());
    let native_array = set
        .iter()
        .flat_map(|v| match v {
            Decimal128(v128op, _, _) => *v128op,
            datatype => {
                unreachable!("InList can't reach other data type {} for {}.", datatype, v)
            }
        })
        .collect::<Vec<_>>();
    let native_set: HashSet<i128> = HashSet::from_iter(native_array);

    collection_contains_check_decimal!(array, native_set, negated, contains_null)
}

fn set_contains_utf8<OffsetSize: OffsetSizeTrait>(
    array: &GenericStringArray<OffsetSize>,
    set: &HashSet<ScalarValue>,
    negated: bool,
) -> ColumnarValue {
    let contains_null = set.iter().any(|v| v.is_null());
    let native_array = set
        .iter()
        .flat_map(|v| match v {
            Utf8(v) => v.as_deref(),
            LargeUtf8(v) => v.as_deref(),
            datatype => {
                unreachable!("InList can't reach other data type {} for {}.", datatype, v)
            }
        })
        .collect::<Vec<_>>();
    let native_set: HashSet<&str> = HashSet::from_iter(native_array);

    collection_contains_check!(array, native_set, negated, contains_null)
}

impl InListExpr {
    /// Create a new InList expression
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        list: Vec<Arc<dyn PhysicalExpr>>,
        negated: bool,
        schema: &Schema,
    ) -> Self {
        if list.len() > OPTIMIZER_INSET_THRESHOLD {
            if let Ok(set) = try_cast_static_filter_to_set(&list, schema) {
                return Self {
                    expr,
                    set: Some(InSet::new(set)),
                    list,
                    negated,
                };
            }
        }
        Self {
            expr,
            list,
            negated,
            set: None,
        }
    }

    /// Input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// List to search in
    pub fn list(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.list
    }

    /// Is this negated e.g. NOT IN LIST
    pub fn negated(&self) -> bool {
        self.negated
    }

    /// Compare for specific utf8 types
    #[allow(clippy::unnecessary_wraps)]
    fn compare_utf8<T: OffsetSizeTrait>(
        &self,
        array: ArrayRef,
        list_values: Vec<ColumnarValue>,
        negated: bool,
    ) -> Result<ColumnarValue> {
        let array = array
            .as_any()
            .downcast_ref::<GenericStringArray<T>>()
            .unwrap();

        let contains_null = list_values
            .iter()
            .any(|v| matches!(v, ColumnarValue::Scalar(s) if s.is_null()));
        let values = list_values
            .iter()
            .flat_map(|expr| match expr {
                ColumnarValue::Scalar(s) => match s {
                    ScalarValue::Utf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::Utf8(None) => None,
                    ScalarValue::LargeUtf8(Some(v)) => Some(v.as_str()),
                    ScalarValue::LargeUtf8(None) => None,
                    datatype => unimplemented!("Unexpected type {} for InList", datatype),
                },
                ColumnarValue::Array(_) => {
                    unimplemented!("InList does not yet support nested columns.")
                }
            })
            .collect::<Vec<&str>>();

        if negated {
            if contains_null {
                Ok(ColumnarValue::Array(Arc::new(
                    array
                        .iter()
                        .map(|x| match x.map(|v| !values.contains(&v)) {
                            Some(true) => None,
                            x => x,
                        })
                        .collect::<BooleanArray>(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(not_in_list_utf8(
                    array, &values,
                )?)))
            }
        } else if contains_null {
            Ok(ColumnarValue::Array(Arc::new(
                array
                    .iter()
                    .map(|x| match x.map(|v| values.contains(&v)) {
                        Some(false) => None,
                        x => x,
                    })
                    .collect::<BooleanArray>(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(in_list_utf8(
                array, &values,
            )?)))
        }
    }
}

impl std::fmt::Display for InListExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.negated {
            if self.set.is_some() {
                write!(f, "{} NOT IN (SET) ({:?})", self.expr, self.list)
            } else {
                write!(f, "{} NOT IN ({:?})", self.expr, self.list)
            }
        } else if self.set.is_some() {
            write!(f, "Use {} IN (SET) ({:?})", self.expr, self.list)
        } else {
            write!(f, "{} IN ({:?})", self.expr, self.list)
        }
    }
}

impl PhysicalExpr for InListExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let value = self.expr.evaluate(batch)?;
        let value_data_type = value.data_type();

        if let Some(in_set) = &self.set {
            let array = match value {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(scalar) => scalar.to_array(),
            };
            let set = in_set.get_set();
            match value_data_type {
                DataType::Boolean => {
                    let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        Boolean,
                        self.negated,
                        bool
                    ))
                }
                DataType::Int8 => {
                    let array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        Int8,
                        self.negated,
                        i8
                    ))
                }
                DataType::Int16 => {
                    let array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        Int16,
                        self.negated,
                        i16
                    ))
                }
                DataType::Int32 => {
                    let array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        Int32,
                        self.negated,
                        i32
                    ))
                }
                DataType::Int64 => {
                    let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        Int64,
                        self.negated,
                        i64
                    ))
                }
                DataType::UInt8 => {
                    let array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        UInt8,
                        self.negated,
                        u8
                    ))
                }
                DataType::UInt16 => {
                    let array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        UInt16,
                        self.negated,
                        u16
                    ))
                }
                DataType::UInt32 => {
                    let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        UInt32,
                        self.negated,
                        u32
                    ))
                }
                DataType::UInt64 => {
                    let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                    Ok(set_contains_for_primitive!(
                        array,
                        set,
                        UInt64,
                        self.negated,
                        u64
                    ))
                }
                DataType::Float32 => {
                    let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                    Ok(set_contains_for_float!(
                        array,
                        set,
                        Float32,
                        self.negated,
                        f32
                    ))
                }
                DataType::Float64 => {
                    let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    Ok(set_contains_for_float!(
                        array,
                        set,
                        Float64,
                        self.negated,
                        f64
                    ))
                }
                DataType::Utf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<GenericStringArray<i32>>()
                        .unwrap();
                    Ok(set_contains_utf8(array, set, self.negated))
                }
                DataType::LargeUtf8 => {
                    let array = array
                        .as_any()
                        .downcast_ref::<GenericStringArray<i64>>()
                        .unwrap();
                    Ok(set_contains_utf8(array, set, self.negated))
                }
                DataType::Decimal128(_, _) => {
                    let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    Ok(make_set_contains_decimal(array, set, self.negated))
                }
                datatype => Result::Err(DataFusionError::NotImplemented(format!(
                    "InSet does not support datatype {:?}.",
                    datatype
                ))),
            }
        } else {
            let list_values = self
                .list
                .iter()
                .map(|expr| expr.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;

            let array = match value {
                ColumnarValue::Array(array) => array,
                ColumnarValue::Scalar(scalar) => scalar.to_array(),
            };

            match value_data_type {
                DataType::Float32 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Float32,
                        Float32Array
                    )
                }
                DataType::Float64 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Float64,
                        Float64Array
                    )
                }
                DataType::Int16 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Int16,
                        Int16Array
                    )
                }
                DataType::Int32 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Int32,
                        Int32Array
                    )
                }
                DataType::Int64 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Int64,
                        Int64Array
                    )
                }
                DataType::Int8 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        Int8,
                        Int8Array
                    )
                }
                DataType::UInt16 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        UInt16,
                        UInt16Array
                    )
                }
                DataType::UInt32 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        UInt32,
                        UInt32Array
                    )
                }
                DataType::UInt64 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        UInt64,
                        UInt64Array
                    )
                }
                DataType::UInt8 => {
                    make_contains_primitive!(
                        array,
                        list_values,
                        self.negated,
                        UInt8,
                        UInt8Array
                    )
                }
                DataType::Boolean => Ok(make_contains!(
                    array,
                    list_values,
                    self.negated,
                    Boolean,
                    BooleanArray
                )),
                DataType::Utf8 => {
                    self.compare_utf8::<i32>(array, list_values, self.negated)
                }
                DataType::LargeUtf8 => {
                    self.compare_utf8::<i64>(array, list_values, self.negated)
                }
                DataType::Null => {
                    let null_array = new_null_array(&DataType::Boolean, array.len());
                    Ok(ColumnarValue::Array(Arc::new(null_array)))
                }
                DataType::Decimal128(_, _) => {
                    let decimal_array =
                        array.as_any().downcast_ref::<Decimal128Array>().unwrap();
                    Ok(make_list_contains_decimal(
                        decimal_array,
                        list_values,
                        self.negated,
                    ))
                }
                datatype => Result::Err(DataFusionError::NotImplemented(format!(
                    "InList does not support datatype {:?}.",
                    datatype
                ))),
            }
        }
    }
}

/// Creates a unary expression InList
pub fn in_list(
    expr: Arc<dyn PhysicalExpr>,
    list: Vec<Arc<dyn PhysicalExpr>>,
    negated: &bool,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(InListExpr::new(expr, list, *negated, schema)))
}

#[cfg(test)]
mod tests {
    use arrow::{array::StringArray, datatypes::Field};

    use super::*;
    use crate::expressions;
    use crate::expressions::{col, lit};
    use crate::planner::in_list_cast;
    use datafusion_common::Result;

    // applies the in_list expr to an input batch and list
    macro_rules! in_list {
        ($BATCH:expr, $LIST:expr, $NEGATED:expr, $EXPECTED:expr, $COL:expr, $SCHEMA:expr) => {{
            let (cast_expr, cast_list_exprs) = in_list_cast($COL, $LIST, $SCHEMA)?;
            let expr = in_list(cast_expr, cast_list_exprs, $NEGATED, $SCHEMA).unwrap();
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
    fn in_list_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("d"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b")];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in ("a", "b")"
        let list = vec![lit("a"), lit("b"), lit(ScalarValue::Utf8(None))];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1)"
        let list = vec![lit(0i64), lit(1i64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0, 1, NULL)"
        let list = vec![lit(0i64), lit(1i64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![Some(0.0), Some(0.2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0.0, 0.2)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0.0, 0.2)"
        let list = vec![lit(0.0f64), lit(0.1f64)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (0.0, 0.2, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (0.0, 0.2, NULL)"
        let list = vec![lit(0.0f64), lit(0.1f64), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (true)"
        let list = vec![lit(true)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (true, NULL)"
        let list = vec![lit(true), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_decimal() -> Result<()> {
        // Now, we can check the NULL type
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);
        let array = vec![Some(100_0000_i128), None, Some(200_5000_i128)]
            .into_iter()
            .collect::<Decimal128Array>();
        let array = array.with_precision_and_scale(13, 4).unwrap();
        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;

        // expression: "a in (100,200), the data type of list is INT32
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );
        // expression: "a not in (100,200)
        let list = vec![lit(100i32), lit(200i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            col_a.clone(),
            &schema
        );

        // expression: "a in (200,NULL), the data type of list is INT32 AND NULL
        let list = vec![lit(ScalarValue::Int32(Some(100))), lit(ScalarValue::Null)];
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );
        // expression: "a not in (200,NULL), the data type of list is INT32 AND NULL
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        // expression: "a in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(100i32)];
        in_list!(
            batch,
            list,
            &false,
            vec![Some(true), None, Some(true)],
            col_a.clone(),
            &schema
        );

        // expression: "a not in (200.5, 100), the data type of list is FLOAT32 and INT32
        let list = vec![lit(200.50f32), lit(101i32)];
        in_list!(
            batch,
            list,
            &true,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );

        // test the optimization: set
        // expression: "a in (99..300), the data type of list is INT32
        let list = (99i32..300).into_iter().map(lit).collect::<Vec<_>>();

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, Some(false)],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, Some(true)],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_set_bool() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, true)]);
        let a = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (true,null,true.....)"
        let mut list = vec![
            lit(ScalarValue::Boolean(Some(true))),
            lit(ScalarValue::Boolean(None)),
        ];
        for _ in 0..OPTIMIZER_INSET_THRESHOLD {
            list.push(lit(ScalarValue::Boolean(Some(true))));
        }
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );
        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );
        Ok(())
    }

    #[test]
    fn in_list_set_int64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let a = Int64Array::from(vec![Some(0), Some(2), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0,NULL,3,4....)"
        let mut list = vec![
            lit(ScalarValue::Int64(Some(0))),
            lit(ScalarValue::Int64(None)),
            lit(ScalarValue::Int64(Some(3))),
        ];
        for v in 4..(OPTIMIZER_INSET_THRESHOLD + 4) {
            list.push(lit(ScalarValue::Int64(Some(v as i64))));
        }

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list.clone(),
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_set_float64() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let a = Float64Array::from(vec![Some(0.0), Some(2.0), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in (0.0,NULL,3.0,4.0 ....)"
        let mut list = vec![
            lit(ScalarValue::Float64(Some(0.0))),
            lit(ScalarValue::Float64(None)),
            lit(ScalarValue::Float64(Some(3.0))),
        ];
        for v in 4..(OPTIMIZER_INSET_THRESHOLD + 4) {
            list.push(lit(ScalarValue::Float64(Some(v as f64))));
        }

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list.clone(),
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_set_utf8() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("a"), Some("b"), None]);
        let col_a = col("a", &schema)?;
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(a)])?;

        // expression: "a in ("a", NULL, "4c", "5c", ....)"
        let mut list = vec![
            lit(ScalarValue::Utf8(Some("a".to_string()))),
            lit(ScalarValue::Utf8(None)),
        ];
        for v in 4..(OPTIMIZER_INSET_THRESHOLD + 4) {
            let value = v.to_string() + "c";
            list.push(lit(ScalarValue::Utf8(Some(value))));
        }
        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list.clone(),
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );

        Ok(())
    }

    #[test]
    fn in_list_set_decimal() -> Result<()> {
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);
        let array = vec![Some(100_0000_i128), Some(200_5000_i128), None]
            .into_iter()
            .collect::<Decimal128Array>();
        let array = array.with_precision_and_scale(13, 4).unwrap();
        let col_a = col("a", &schema)?;
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])?;

        // expression: "a in (100.0000, Null, 100.0004, 100.0005...)
        let mut list = vec![
            lit(ScalarValue::Decimal128(Some(100_0000_i128), 13, 4)),
            lit(ScalarValue::Decimal128(None, 13, 4)),
        ];
        for v in 4..(OPTIMIZER_INSET_THRESHOLD + 4) {
            let value = 100_0000_i128 + v as i128;
            list.push(lit(ScalarValue::Decimal128(Some(value), 13, 4)));
        }

        in_list!(
            batch,
            list.clone(),
            &false,
            vec![Some(true), None, None],
            col_a.clone(),
            &schema
        );

        in_list!(
            batch,
            list,
            &true,
            vec![Some(false), None, None],
            col_a.clone(),
            &schema
        );
        Ok(())
    }

    #[test]
    fn test_cast_static_filter_to_set() -> Result<()> {
        // random schema
        let schema =
            Schema::new(vec![Field::new("a", DataType::Decimal128(13, 4), true)]);
        // list of phy expr
        let mut phy_exprs = vec![
            lit(1i64),
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            expressions::try_cast(lit(3.13f32), &schema, DataType::Int64)?,
        ];
        let result = try_cast_static_filter_to_set(&phy_exprs, &schema).unwrap();

        assert!(result.contains(&1i64.try_into().unwrap()));
        assert!(result.contains(&2i64.try_into().unwrap()));
        assert!(result.contains(&3i64.try_into().unwrap()));

        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_ok());
        // cast(cast(lit())), but the cast to the same data type, one case will be ignored
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int64,
        )?);
        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_ok());
        // case(cast(lit())), the cast to the diff data type
        phy_exprs.push(expressions::cast(
            expressions::cast(lit(2i32), &schema, DataType::Int64)?,
            &schema,
            DataType::Int32,
        )?);
        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_ok());

        // column
        phy_exprs.push(expressions::col("a", &schema)?);
        assert!(try_cast_static_filter_to_set(&phy_exprs, &schema).is_err());

        Ok(())
    }
}
