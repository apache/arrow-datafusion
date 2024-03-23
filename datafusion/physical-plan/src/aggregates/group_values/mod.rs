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

use arrow::record_batch::RecordBatch;
use arrow_array::{downcast_primitive, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::Result;

pub(crate) mod primitive;
use datafusion_expr::EmitTo;
use primitive::GroupValuesPrimitive;

mod row;
use row::GroupValuesRows;

mod bytes;
mod fully_ordered;

use crate::aggregates::group_values::fully_ordered::GroupValuesFullyOrdered;
use bytes::GroupValuesByes;
use datafusion_physical_expr::binary_map::OutputType;
use datafusion_physical_expr::PhysicalSortExpr;

/// An interning store for group keys
pub trait GroupValues: Send {
    /// Calculates the `groups` for each input row of `cols`
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes used by this [`GroupValues`]
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, batch: &RecordBatch);
}

pub fn new_group_values(
    schema: SchemaRef,
    ordering: Option<Vec<PhysicalSortExpr>>,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new($d.clone())))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            _ => {}
        }

        if let DataType::Utf8 = d {
            return Ok(Box::new(GroupValuesByes::<i32>::new(OutputType::Utf8)));
        }
        if let DataType::LargeUtf8 = d {
            return Ok(Box::new(GroupValuesByes::<i64>::new(OutputType::Utf8)));
        }
        if let DataType::Binary = d {
            return Ok(Box::new(GroupValuesByes::<i32>::new(OutputType::Binary)));
        }
        if let DataType::LargeBinary = d {
            return Ok(Box::new(GroupValuesByes::<i64>::new(OutputType::Binary)));
        }
    } else if let Some(ordering) = ordering {
        return Ok(Box::new(GroupValuesFullyOrdered::try_new(
            schema, ordering,
        )?));
    }

    Ok(Box::new(GroupValuesRows::try_new(schema)?))
}
