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

//! get field of a `ListArray`

use crate::PhysicalExpr;
use datafusion_common::{exec_err, plan_err};

use crate::array_expressions::{array_element, array_slice};
use crate::physical_expr::down_cast_any_ref;
use arrow::{
    array::{Array, Scalar, StringArray},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{
    cast::{as_map_array, as_struct_array},
    DataFusionError, Result, ScalarValue,
};
use datafusion_expr::{field_util::GetFieldAccessSchema, ColumnarValue};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

/// Access a sub field of a nested type, such as `Field` or `List`
#[derive(Clone, Hash, Debug)]
pub enum GetFieldAccessExpr {
    /// Named field, For example `struct["name"]`
    NamedStructField { name: ScalarValue },
    /// Single list index, for example: `list[i]`
    ListIndex { key: Arc<dyn PhysicalExpr> },
    /// List range, for example `list[i:j]`
    ListRange {
        start: Arc<dyn PhysicalExpr>,
        stop: Arc<dyn PhysicalExpr>,
    },
}

impl std::fmt::Display for GetFieldAccessExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GetFieldAccessExpr::NamedStructField { name } => write!(f, "[{}]", name),
            GetFieldAccessExpr::ListIndex { key } => write!(f, "[{}]", key),
            GetFieldAccessExpr::ListRange { start, stop } => {
                write!(f, "[{}:{}]", start, stop)
            }
        }
    }
}

impl PartialEq<dyn Any> for GetFieldAccessExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        use GetFieldAccessExpr::{ListIndex, ListRange, NamedStructField};
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| match (self, x) {
                (NamedStructField { name: lhs }, NamedStructField { name: rhs }) => {
                    lhs.eq(rhs)
                }
                (ListIndex { key: lhs }, ListIndex { key: rhs }) => lhs.eq(rhs),
                (
                    ListRange {
                        start: start_lhs,
                        stop: stop_lhs,
                    },
                    ListRange {
                        start: start_rhs,
                        stop: stop_rhs,
                    },
                ) => start_lhs.eq(start_rhs) && stop_lhs.eq(stop_rhs),
                (NamedStructField { .. }, ListIndex { .. } | ListRange { .. }) => false,
                (ListIndex { .. }, NamedStructField { .. } | ListRange { .. }) => false,
                (ListRange { .. }, NamedStructField { .. } | ListIndex { .. }) => false,
            })
            .unwrap_or(false)
    }
}

/// Expression to get a field of a struct array.
#[derive(Debug, Hash)]
pub struct GetIndexedFieldExpr {
    /// The expression to find
    arg: Arc<dyn PhysicalExpr>,
    /// The key statement
    field: GetFieldAccessExpr,
}

impl GetIndexedFieldExpr {
    /// Create new [`GetIndexedFieldExpr`]
    pub fn new(arg: Arc<dyn PhysicalExpr>, field: GetFieldAccessExpr) -> Self {
        Self { arg, field }
    }

    /// Create a new [`GetIndexedFieldExpr`] for accessing the named field
    pub fn new_field(arg: Arc<dyn PhysicalExpr>, name: impl Into<String>) -> Self {
        Self::new(
            arg,
            GetFieldAccessExpr::NamedStructField {
                name: ScalarValue::Utf8(Some(name.into())),
            },
        )
    }

    /// Create a new [`GetIndexedFieldExpr`] for accessing the specified index
    pub fn new_index(arg: Arc<dyn PhysicalExpr>, key: Arc<dyn PhysicalExpr>) -> Self {
        Self::new(arg, GetFieldAccessExpr::ListIndex { key })
    }

    /// Create a new [`GetIndexedFieldExpr`] for accessing the range
    pub fn new_range(
        arg: Arc<dyn PhysicalExpr>,
        start: Arc<dyn PhysicalExpr>,
        stop: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self::new(arg, GetFieldAccessExpr::ListRange { start, stop })
    }

    /// Get the description of what field should be accessed
    pub fn field(&self) -> &GetFieldAccessExpr {
        &self.field
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }

    fn schema_access(&self, input_schema: &Schema) -> Result<GetFieldAccessSchema> {
        Ok(match &self.field {
            GetFieldAccessExpr::NamedStructField { name } => {
                GetFieldAccessSchema::NamedStructField { name: name.clone() }
            }
            GetFieldAccessExpr::ListIndex { key } => {
                let data_type = key.data_type(input_schema)?;
                match data_type {
                    DataType::Int64 => {}
                    _ => {
                        return plan_err!(
                            "Only ints are valid as an indexed field in a list"
                        );
                    }
                }

                GetFieldAccessSchema::ListIndex
            }
            GetFieldAccessExpr::ListRange { start, stop } => {
                match (
                    start.data_type(input_schema)?,
                    stop.data_type(input_schema)?,
                ) {
                    (DataType::Int64, DataType::Int64) => {}
                    _ => {
                        return plan_err!(
                            "Only ints are valid as an indexed field in a list"
                        );
                    }
                }

                GetFieldAccessSchema::ListRange
            }
        })
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).{}", self.arg, self.field)
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let arg_dt = self.arg.data_type(input_schema)?;
        self.schema_access(input_schema)?
            .get_accessed_field(&arg_dt)
            .map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let arg_dt = self.arg.data_type(input_schema)?;
        self.schema_access(input_schema)?
            .get_accessed_field(&arg_dt)
            .map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let array = self.arg.evaluate(batch)?.into_array(batch.num_rows())?;
        match &self.field {
            GetFieldAccessExpr::NamedStructField{name} => match (array.data_type(), name) {
                (DataType::Map(_, _), ScalarValue::Utf8(Some(k))) => {
                    let map_array = as_map_array(array.as_ref())?;
                    let key_scalar = Scalar::new(StringArray::from(vec![k.clone()]));
                    let keys = arrow::compute::kernels::cmp::eq(&key_scalar, map_array.keys())?;
                    let entries = arrow::compute::filter(map_array.entries(), &keys)?;
                    let entries_struct_array = as_struct_array(entries.as_ref())?;
                    Ok(ColumnarValue::Array(entries_struct_array.column(1).clone()))
                }
                (DataType::Struct(_), ScalarValue::Utf8(Some(k))) => {
                    let as_struct_array = as_struct_array(&array)?;
                    match as_struct_array.column_by_name(k) {
                        None => exec_err!(
                            "get indexed field {k} not found in struct"),
                        Some(col) => Ok(ColumnarValue::Array(col.clone()))
                    }
                }
                (DataType::Struct(_), name) => exec_err!(
                    "get indexed field is only possible on struct with utf8 indexes. \
                             Tried with {name:?} index"),
                (dt, name) => exec_err!(
                                "get indexed field is only possible on lists with int64 indexes or struct \
                                         with utf8 indexes. Tried {dt:?} with {name:?} index"),
            },
            GetFieldAccessExpr::ListIndex{key} => {
            let key = key.evaluate(batch)?.into_array(batch.num_rows())?;
            match (array.data_type(), key.data_type()) {
                (DataType::List(_), DataType::Int64) => Ok(ColumnarValue::Array(array_element(&[
                    array, key
                ])?)),
                (DataType::List(_), key) => exec_err!(
                                "get indexed field is only possible on lists with int64 indexes. \
                                    Tried with {key:?} index"),
                            (dt, key) => exec_err!(
                                        "get indexed field is only possible on lists with int64 indexes or struct \
                                                 with utf8 indexes. Tried {dt:?} with {key:?} index"),
                        }
                },
            GetFieldAccessExpr::ListRange{start, stop} => {
                let start = start.evaluate(batch)?.into_array(batch.num_rows())?;
                let stop = stop.evaluate(batch)?.into_array(batch.num_rows())?;
                match (array.data_type(), start.data_type(), stop.data_type()) {
                    (DataType::List(_), DataType::Int64, DataType::Int64) => Ok(ColumnarValue::Array(array_slice(&[
                        array, start, stop
                    ])?)),
                    (DataType::List(_), start, stop) => exec_err!(
                        "get indexed field is only possible on lists with int64 indexes. \
                                 Tried with {start:?} and {stop:?} indices"),
                    (dt, start, stop) => exec_err!(
                        "get indexed field is only possible on lists with int64 indexes or struct \
                                 with utf8 indexes. Tried {dt:?} with {start:?} and {stop:?} indices"),
                }
            },
        }
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.arg.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(GetIndexedFieldExpr::new(
            children[0].clone(),
            self.field.clone(),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for GetIndexedFieldExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg) && self.field.eq(&x.field))
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::array::new_empty_array;
    use arrow::array::{ArrayRef, GenericListArray};
    use arrow::array::{
        BooleanArray, Int64Array, ListBuilder, StringBuilder, StructArray,
    };
    use arrow::datatypes::Fields;
    use arrow::{array::StringArray, datatypes::Field};
    use datafusion_common::cast::{as_boolean_array, as_list_array, as_string_array};
    use datafusion_common::Result;

    fn build_list_arguments(
        list_of_lists: Vec<Vec<Option<&str>>>,
        list_of_start_indices: Vec<Option<i64>>,
        list_of_stop_indices: Vec<Option<i64>>,
    ) -> (GenericListArray<i32>, Int64Array, Int64Array) {
        let builder = StringBuilder::with_capacity(list_of_lists.len(), 1024);
        let mut list_builder = ListBuilder::new(builder);
        for values in list_of_lists {
            let builder = list_builder.values();
            for value in values {
                match value {
                    None => builder.append_null(),
                    Some(v) => builder.append_value(v),
                }
            }
            list_builder.append(true);
        }

        let start_array = Int64Array::from(list_of_start_indices);
        let stop_array = Int64Array::from(list_of_stop_indices);
        (list_builder.finish(), start_array, stop_array)
    }

    #[test]
    fn get_indexed_field_named_struct_field() -> Result<()> {
        let schema = struct_schema();
        let boolean = BooleanArray::from(vec![false, false, true, true]);
        let int = Int64Array::from(vec![42, 28, 19, 31]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("a", DataType::Boolean, true)),
                Arc::new(boolean.clone()) as ArrayRef,
            ),
            (
                Arc::new(Field::new("b", DataType::Int64, true)),
                Arc::new(int) as ArrayRef,
            ),
        ]);
        let expr = col("str", &schema).unwrap();
        // only one row should be processed
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(struct_array)])?;
        let expr = Arc::new(GetIndexedFieldExpr::new_field(expr, "a"));
        let result = expr
            .evaluate(&batch)?
            .into_array(1)
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");
        assert_eq!(boolean, result.clone());
        Ok(())
    }

    fn struct_schema() -> Schema {
        Schema::new(vec![Field::new_struct(
            "str",
            Fields::from(vec![
                Field::new("a", DataType::Boolean, true),
                Field::new("b", DataType::Int64, true),
            ]),
            true,
        )])
    }

    fn list_schema(cols: &[&str]) -> Schema {
        if cols.len() == 2 {
            Schema::new(vec![
                Field::new_list(cols[0], Field::new("item", DataType::Utf8, true), true),
                Field::new(cols[1], DataType::Int64, true),
            ])
        } else {
            Schema::new(vec![
                Field::new_list(cols[0], Field::new("item", DataType::Utf8, true), true),
                Field::new(cols[1], DataType::Int64, true),
                Field::new(cols[2], DataType::Int64, true),
            ])
        }
    }

    #[test]
    fn get_indexed_field_list_index() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let list_of_start_indices = vec![Some(1), Some(2), None];
        let list_of_stop_indices = vec![None];
        let expected_list = vec![Some("a"), Some("c"), None];

        let schema = list_schema(&["list", "key"]);
        let (list_col, key_col, _) = build_list_arguments(
            list_of_lists,
            list_of_start_indices,
            list_of_stop_indices,
        );
        let expr = col("list", &schema).unwrap();
        let key = col("key", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_col), Arc::new(key_col)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new_index(expr, key));
        let result = expr
            .evaluate(&batch)?
            .into_array(1)
            .expect("Failed to convert to array");
        let result = as_string_array(&result).expect("failed to downcast to ListArray");
        let expected = StringArray::from(expected_list);
        assert_eq!(expected, result.clone());
        Ok(())
    }

    #[test]
    fn get_indexed_field_list_range() -> Result<()> {
        let list_of_lists = vec![
            vec![Some("a"), Some("b"), None],
            vec![None, Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];
        let list_of_start_indices = vec![Some(1), Some(2), None];
        let list_of_stop_indices = vec![Some(2), None, Some(3)];
        let expected_list = vec![
            vec![Some("a"), Some("b")],
            vec![Some("c"), Some("d")],
            vec![Some("e"), None, Some("f")],
        ];

        let schema = list_schema(&["list", "start", "stop"]);
        let (list_col, start_col, stop_col) = build_list_arguments(
            list_of_lists,
            list_of_start_indices,
            list_of_stop_indices,
        );
        let expr = col("list", &schema).unwrap();
        let start = col("start", &schema).unwrap();
        let stop = col("stop", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_col), Arc::new(start_col), Arc::new(stop_col)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new_range(expr, start, stop));
        let result = expr
            .evaluate(&batch)?
            .into_array(1)
            .expect("Failed to convert to array");
        let result = as_list_array(&result).expect("failed to downcast to ListArray");
        let (expected, _, _) =
            build_list_arguments(expected_list, vec![None], vec![None]);
        assert_eq!(expected, result.clone());
        Ok(())
    }

    #[test]
    fn get_indexed_field_empty_list() -> Result<()> {
        let schema = list_schema(&["list", "key"]);
        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);
        let key_array = new_empty_array(&DataType::Int64);
        let expr = col("list", &schema).unwrap();
        let key = col("key", &schema).unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), key_array],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new_index(expr, key));
        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn get_indexed_field_invalid_list_index() -> Result<()> {
        let schema = list_schema(&["list", "error"]);
        let expr = col("list", &schema).unwrap();
        let key = col("error", &schema).unwrap();
        let builder = StringBuilder::with_capacity(3, 1024);
        let mut list_builder = ListBuilder::new(builder);
        list_builder.values().append_value("hello");
        list_builder.append(true);

        let key_array = Int64Array::from(vec![Some(3)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(list_builder.finish()), Arc::new(key_array)],
        )?;
        let expr = Arc::new(GetIndexedFieldExpr::new_index(expr, key));
        let result = expr
            .evaluate(&batch)?
            .into_array(1)
            .expect("Failed to convert to array");
        assert!(result.is_null(0));
        Ok(())
    }

    #[test]
    fn get_indexed_field_eq() -> Result<()> {
        let schema = list_schema(&["list", "error"]);
        let expr = col("list", &schema).unwrap();
        let key = col("error", &schema).unwrap();
        let indexed_field =
            Arc::new(GetIndexedFieldExpr::new_index(expr.clone(), key.clone()))
                as Arc<dyn PhysicalExpr>;
        let indexed_field_other =
            Arc::new(GetIndexedFieldExpr::new_index(key, expr)) as Arc<dyn PhysicalExpr>;
        assert!(indexed_field.eq(&indexed_field));
        assert!(!indexed_field.eq(&indexed_field_other));
        Ok(())
    }
}
