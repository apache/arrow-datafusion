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

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use crate::metrics::BaselineMetrics;
use crate::sorts::{
    cascade::SortPreservingCascadeStream,
    stream::{FieldCursorStream, RowCursorStream},
};
use crate::{PhysicalSortExpr, SendableRecordBatchStream};
use arrow::datatypes::{DataType, SchemaRef};
use arrow_array::*;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;

macro_rules! primitive_merge_helper {
    ($t:ty, $($v:ident),+) => {
        merge_helper!(PrimitiveArray<$t>, $($v),+)
    };
}

macro_rules! merge_helper {
    ($t:ty, $sort:ident, $streams:ident, $schema:ident, $tracking_metrics:ident, $batch_size:ident, $fetch:ident, $reservation:ident) => {{
        let streams = FieldCursorStream::<$t>::new($sort, $streams);
        return Ok(Box::pin(SortPreservingCascadeStream::new(
            streams,
            $schema,
            $tracking_metrics,
            $batch_size,
            $fetch,
            $reservation,
        )));
    }};
}

/// Perform a streaming merge of [`SendableRecordBatchStream`] based on provided sort expressions
/// while preserving order.
pub fn streaming_merge(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    expressions: &[PhysicalSortExpr],
    metrics: BaselineMetrics,
    batch_size: usize,
    fetch: Option<usize>,
    reservation: MemoryReservation,
) -> Result<SendableRecordBatchStream> {
    // Special case single column comparisons with optimized cursor implementations
    if expressions.len() == 1 {
        let sort = expressions[0].clone();
        let data_type = sort.expr.data_type(schema.as_ref())?;
        downcast_primitive! {
            data_type => (primitive_merge_helper, sort, streams, schema, metrics, batch_size, fetch, reservation),
            DataType::Utf8 => merge_helper!(StringArray, sort, streams, schema, metrics, batch_size, fetch, reservation)
            DataType::LargeUtf8 => merge_helper!(LargeStringArray, sort, streams, schema, metrics, batch_size, fetch, reservation)
            DataType::Binary => merge_helper!(BinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation)
            DataType::LargeBinary => merge_helper!(LargeBinaryArray, sort, streams, schema, metrics, batch_size, fetch, reservation)
            _ => {}
        }
    }

    let streams = RowCursorStream::try_new(
        schema.as_ref(),
        expressions,
        streams,
        reservation.new_empty(),
    )?;

    Ok(Box::pin(SortPreservingCascadeStream::new(
        streams,
        schema,
        metrics,
        batch_size,
        fetch,
        reservation,
    )))
}
