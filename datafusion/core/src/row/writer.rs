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

//! Reusable row writer backed by Vec<u8> to stitch attributes together

#[cfg(feature = "jit")]
use crate::error::Result;
#[cfg(feature = "jit")]
use crate::reg_fn;
#[cfg(feature = "jit")]
use crate::row::fn_name;
use crate::row::{
    estimate_row_width, fixed_size, get_offsets, row_supported, schema_null_free,
};
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::bit_util::{ceil, round_upto_power_of_2, set_bit_raw, unset_bit_raw};
#[cfg(feature = "jit")]
use datafusion_jit::api::CodeBlock;
#[cfg(feature = "jit")]
use datafusion_jit::api::{Assembler, GeneratedFunction};
#[cfg(feature = "jit")]
use datafusion_jit::ast::Expr;
#[cfg(feature = "jit")]
use datafusion_jit::ast::{BOOL, I64, PTR};
use std::cmp::max;
use std::sync::Arc;

/// Append batch from `row_idx` to `output` buffer start from `offset`
/// # Panics
///
/// This function will panic if the output buffer doesn't have enough space to hold all the rows
pub fn write_batch_unchecked(
    output: &mut [u8],
    offset: usize,
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Schema>,
) -> Vec<usize> {
    let mut writer = RowWriter::new(&schema);
    let mut current_offset = offset;
    let mut offsets = vec![];
    let columns = batch.columns();
    for cur_row in row_idx..batch.num_rows() {
        offsets.push(current_offset);
        let row_width = write_row(&mut writer, cur_row, &schema, columns);
        output[current_offset..current_offset + row_width]
            .copy_from_slice(writer.get_row());
        current_offset += row_width;
        writer.reset()
    }
    offsets
}

/// Append batch from `row_idx` to `output` buffer start from `offset`
/// # Panics
///
/// This function will panic if the output buffer doesn't have enough space to hold all the rows
#[cfg(feature = "jit")]
pub fn write_batch_unchecked_jit(
    output: &mut [u8],
    offset: usize,
    batch: &RecordBatch,
    row_idx: usize,
    schema: Arc<Schema>,
    assembler: &Assembler,
) -> Result<Vec<usize>> {
    let mut writer = RowWriter::new(&schema);
    let mut current_offset = offset;
    let mut offsets = vec![];
    register_write_functions(assembler)?;
    let gen_func = gen_write_row(&schema, assembler)?;
    let mut jit = assembler.create_jit();
    let code_ptr = jit.compile(gen_func)?;

    let code_fn = unsafe {
        std::mem::transmute::<_, fn(&mut RowWriter, usize, &RecordBatch)>(code_ptr)
    };

    for cur_row in row_idx..batch.num_rows() {
        offsets.push(current_offset);
        code_fn(&mut writer, cur_row, batch);
        writer.end_padding();
        let row_width = writer.row_width;
        output[current_offset..current_offset + row_width]
            .copy_from_slice(writer.get_row());
        current_offset += row_width;
        writer.reset()
    }
    Ok(offsets)
}

#[cfg(feature = "jit")]
/// bench interpreted version write
#[inline(never)]
pub fn bench_write_batch(
    batches: &[Vec<RecordBatch>],
    schema: Arc<Schema>,
) -> Result<Vec<usize>> {
    let mut writer = RowWriter::new(&schema);
    let mut lengths = vec![];

    for batch in batches.iter().flatten() {
        let columns = batch.columns();
        for cur_row in 0..batch.num_rows() {
            let row_width = write_row(&mut writer, cur_row, &schema, columns);
            lengths.push(row_width);
            writer.reset()
        }
    }

    Ok(lengths)
}

#[cfg(feature = "jit")]
/// bench jit version write
#[inline(never)]
pub fn bench_write_batch_jit(
    batches: &[Vec<RecordBatch>],
    schema: Arc<Schema>,
) -> Result<Vec<usize>> {
    let assembler = Assembler::default();
    let mut writer = RowWriter::new(&schema);
    let mut lengths = vec![];
    register_write_functions(&assembler)?;
    let gen_func = gen_write_row(&schema, &assembler)?;
    let mut jit = assembler.create_jit();
    let code_ptr = jit.compile(gen_func)?;
    let code_fn = unsafe {
        std::mem::transmute::<_, fn(&mut RowWriter, usize, &RecordBatch)>(code_ptr)
    };

    for batch in batches.iter().flatten() {
        for cur_row in 0..batch.num_rows() {
            code_fn(&mut writer, cur_row, batch);
            writer.end_padding();
            lengths.push(writer.row_width);
            writer.reset()
        }
    }
    Ok(lengths)
}

macro_rules! set_idx {
    ($WIDTH: literal, $SELF: ident, $IDX: ident, $VALUE: ident) => {{
        $SELF.assert_index_valid($IDX);
        let offset = $SELF.field_offsets[$IDX];
        $SELF.data[offset..offset + $WIDTH].copy_from_slice(&$VALUE.to_le_bytes());
    }};
}

macro_rules! fn_set_idx {
    ($NATIVE: ident, $WIDTH: literal) => {
        paste::item! {
            fn [<set_ $NATIVE>](&mut self, idx: usize, value: $NATIVE) {
                self.assert_index_valid(idx);
                let offset = self.field_offsets[idx];
                self.data[offset..offset + $WIDTH].copy_from_slice(&value.to_le_bytes());
            }
        }
    };
}

/// Reusable row writer backed by Vec<u8>
pub struct RowWriter {
    /// buffer for the current tuple been written.
    data: Vec<u8>,
    /// Total number of fields for each tuple.
    field_count: usize,
    /// Length in bytes for the current tuple, 8-bytes word aligned.
    row_width: usize,
    /// The number of bytes used to store null bits for each field.
    null_width: usize,
    /// Length in bytes for `values` part of the current tuple.
    values_width: usize,
    /// Length in bytes for `variable length data` part of the current tuple.
    varlena_width: usize,
    /// Current offset for the next variable length field to write to.
    varlena_offset: usize,
    /// Starting offset for each fields in the raw bytes.
    /// For fixed length fields, it's where the actual data stores.
    /// For variable length fields, it's a pack of (offset << 32 | length) if we use u64.
    field_offsets: Vec<usize>,
    /// If a row is null free according to its schema
    null_free: bool,
}

impl RowWriter {
    /// new
    pub fn new(schema: &Arc<Schema>) -> Self {
        assert!(row_supported(schema));
        let null_free = schema_null_free(schema);
        let field_count = schema.fields().len();
        let null_width = if null_free { 0 } else { ceil(field_count, 8) };
        let (field_offsets, values_width) = get_offsets(null_width, schema);
        let mut init_capacity = estimate_row_width(schema);
        if !fixed_size(schema) {
            // double the capacity to avoid repeated resize
            init_capacity *= 2;
        }
        Self {
            data: vec![0; init_capacity],
            field_count,
            row_width: 0,
            null_width,
            values_width,
            varlena_width: 0,
            varlena_offset: null_width + values_width,
            field_offsets,
            null_free,
        }
    }

    /// Reset the row writer state for new tuple
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.row_width = 0;
        self.varlena_width = 0;
        self.varlena_offset = self.null_width + self.values_width;
    }

    #[inline]
    fn assert_index_valid(&self, idx: usize) {
        assert!(idx < self.field_count);
    }

    fn set_null_at(&mut self, idx: usize) {
        assert!(
            !self.null_free,
            "Unexpected call to set_null_at on null-free row writer"
        );
        let null_bits = &mut self.data[0..self.null_width];
        unsafe {
            unset_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

    fn set_non_null_at(&mut self, idx: usize) {
        assert!(
            !self.null_free,
            "Unexpected call to set_non_null_at on null-free row writer"
        );
        let null_bits = &mut self.data[0..self.null_width];
        unsafe {
            set_bit_raw(null_bits.as_mut_ptr(), idx);
        }
    }

    fn set_bool(&mut self, idx: usize, value: bool) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[offset] = if value { 1 } else { 0 };
    }

    fn set_u8(&mut self, idx: usize, value: u8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[offset] = value;
    }

    fn_set_idx!(u16, 2);
    fn_set_idx!(u32, 4);
    fn_set_idx!(u64, 8);
    fn_set_idx!(i16, 2);
    fn_set_idx!(i32, 4);
    fn_set_idx!(i64, 8);
    fn_set_idx!(f32, 4);
    fn_set_idx!(f64, 8);

    fn set_i8(&mut self, idx: usize, value: i8) {
        self.assert_index_valid(idx);
        let offset = self.field_offsets[idx];
        self.data[offset] = value.to_le_bytes()[0];
    }

    fn set_date32(&mut self, idx: usize, value: i32) {
        set_idx!(4, self, idx, value)
    }

    fn set_date64(&mut self, idx: usize, value: i64) {
        set_idx!(8, self, idx, value)
    }

    fn set_offset_size(&mut self, idx: usize, size: u32) {
        let offset_and_size: u64 = (self.varlena_offset as u64) << 32 | (size as u64);
        self.set_u64(idx, offset_and_size);
    }

    fn set_utf8(&mut self, idx: usize, value: &str) {
        self.assert_index_valid(idx);
        let bytes = value.as_bytes();
        let size = bytes.len();
        self.set_offset_size(idx, size as u32);
        let varlena_offset = self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(bytes);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    fn set_binary(&mut self, idx: usize, value: &[u8]) {
        self.assert_index_valid(idx);
        let size = value.len();
        self.set_offset_size(idx, size as u32);
        let varlena_offset = self.varlena_offset;
        self.data[varlena_offset..varlena_offset + size].copy_from_slice(value);
        self.varlena_offset += size;
        self.varlena_width += size;
    }

    fn current_width(&self) -> usize {
        self.null_width + self.values_width + self.varlena_width
    }

    /// End each row at 8-byte word boundary.
    fn end_padding(&mut self) {
        let payload_width = self.current_width();
        self.row_width = round_upto_power_of_2(payload_width, 8);
        if self.data.capacity() < self.row_width {
            self.data.resize(self.row_width, 0);
        }
    }

    /// Get raw bytes
    pub fn get_row(&self) -> &[u8] {
        &self.data[0..self.row_width]
    }
}

/// Stitch attributes of tuple in `batch` at `row_idx` and returns the tuple width
pub fn write_row(
    row: &mut RowWriter,
    row_idx: usize,
    schema: &Arc<Schema>,
    columns: &[ArrayRef],
) -> usize {
    // Get the row from the batch denoted by row_idx
    if row.null_free {
        for ((i, f), col) in schema.fields().iter().enumerate().zip(columns.iter()) {
            write_field(i, row_idx, col, f.data_type(), row);
        }
    } else {
        for ((i, f), col) in schema.fields().iter().enumerate().zip(columns.iter()) {
            if !col.is_null(row_idx) {
                row.set_non_null_at(i);
                write_field(i, row_idx, col, f.data_type(), row);
            } else {
                row.set_null_at(i);
            }
        }
    }

    row.end_padding();
    row.row_width
}

// we could remove this function wrapper once we find a way to call the trait method directly.
#[cfg(feature = "jit")]
fn is_null(col: &Arc<dyn Array>, row_idx: usize) -> bool {
    col.is_null(row_idx)
}

#[cfg(feature = "jit")]
fn register_write_functions(asm: &Assembler) -> Result<()> {
    let reader_param = vec![PTR, I64, PTR];
    reg_fn!(asm, RecordBatch::column, vec![PTR, I64], Some(PTR));
    reg_fn!(asm, RowWriter::set_null_at, vec![PTR, I64], None);
    reg_fn!(asm, RowWriter::set_non_null_at, vec![PTR, I64], None);
    reg_fn!(asm, is_null, vec![PTR, I64], Some(BOOL));
    reg_fn!(asm, write_field_bool, reader_param.clone(), None);
    reg_fn!(asm, write_field_u8, reader_param.clone(), None);
    reg_fn!(asm, write_field_u16, reader_param.clone(), None);
    reg_fn!(asm, write_field_u32, reader_param.clone(), None);
    reg_fn!(asm, write_field_u64, reader_param.clone(), None);
    reg_fn!(asm, write_field_i8, reader_param.clone(), None);
    reg_fn!(asm, write_field_i16, reader_param.clone(), None);
    reg_fn!(asm, write_field_i32, reader_param.clone(), None);
    reg_fn!(asm, write_field_i64, reader_param.clone(), None);
    reg_fn!(asm, write_field_f32, reader_param.clone(), None);
    reg_fn!(asm, write_field_f64, reader_param.clone(), None);
    reg_fn!(asm, write_field_date32, reader_param.clone(), None);
    reg_fn!(asm, write_field_date64, reader_param.clone(), None);
    reg_fn!(asm, write_field_utf8, reader_param.clone(), None);
    reg_fn!(asm, write_field_binary, reader_param, None);
    Ok(())
}

#[cfg(feature = "jit")]
fn gen_write_row(
    schema: &Arc<Schema>,
    assembler: &Assembler,
) -> Result<GeneratedFunction> {
    let mut builder = assembler
        .new_func_builder("write_row")
        .param("row", PTR)
        .param("row_idx", I64)
        .param("batch", PTR);
    let null_free = schema_null_free(schema);
    let mut b = builder.enter_block();
    for (i, f) in schema.fields().iter().enumerate() {
        let dt = f.data_type();
        let arr = format!("a{}", i);
        b.declare_as(
            &arr,
            b.call("column", vec![b.id("batch")?, b.lit_i(i as i64)])?,
        )?;
        if f.is_nullable() {
            b.if_block(
                |c| c.call("is_null", vec![c.id(&arr)?, c.id("row_idx")?]),
                |t| {
                    t.call_stmt("set_null_at", vec![t.id("row")?, t.lit_i(i as i64)])?;
                    Ok(())
                },
                |e| {
                    e.call_stmt(
                        "set_non_null_at",
                        vec![e.id("row")?, e.lit_i(i as i64)],
                    )?;
                    let params = vec![
                        e.id("row")?,
                        e.id(&arr)?,
                        e.lit_i(i as i64),
                        e.id("row_idx")?,
                    ];
                    write_typed_field_stmt(dt, e, params)?;
                    Ok(())
                },
            )?;
        } else {
            if !null_free {
                b.call_stmt("set_non_null_at", vec![b.id("row")?, b.lit_i(i as i64)])?;
            }
            let params = vec![
                b.id("row")?,
                b.id(&arr)?,
                b.lit_i(i as i64),
                b.id("row_idx")?,
            ];
            write_typed_field_stmt(dt, &mut b, params)?;
        }
    }
    Ok(b.build())
}

#[cfg(feature = "jit")]
fn write_typed_field_stmt<'a>(
    dt: &DataType,
    b: &mut CodeBlock<'a>,
    params: Vec<Expr>,
) -> Result<()> {
    use DataType::*;
    match dt {
        Boolean => b.call_stmt("write_field_bool", params)?,
        UInt8 => b.call_stmt("write_field_u8", params)?,
        UInt16 => b.call_stmt("write_field_u16", params)?,
        UInt32 => b.call_stmt("write_field_u32", params)?,
        UInt64 => b.call_stmt("write_field_u64", params)?,
        Int8 => b.call_stmt("write_field_i8", params)?,
        Int16 => b.call_stmt("write_field_i16", params)?,
        Int32 => b.call_stmt("write_field_i32", params)?,
        Int64 => b.call_stmt("write_field_i64", params)?,
        Float32 => b.call_stmt("write_field_f32", params)?,
        Float64 => b.call_stmt("write_field_f64", params)?,
        Date32 => b.call_stmt("write_field_date32", params)?,
        Date64 => b.call_stmt("write_field_date64", params)?,
        Utf8 => b.call_stmt("write_field_utf8", params)?,
        Binary => b.call_stmt("write_field_binary", params)?,
        _ => unimplemented!(),
    }
    Ok(())
}

macro_rules! fn_write_field {
    ($NATIVE: ident, $ARRAY: ident) => {
        paste::item! {
            fn [<write_field_ $NATIVE>](to: &mut RowWriter, from: &Arc<dyn Array>, col_idx: usize, row_idx: usize) {
                let from = from
                    .as_any()
                    .downcast_ref::<$ARRAY>()
                    .unwrap();
                to.[<set_ $NATIVE>](col_idx, from.value(row_idx));
            }
        }
    };
}

fn_write_field!(bool, BooleanArray);
fn_write_field!(u8, UInt8Array);
fn_write_field!(u16, UInt16Array);
fn_write_field!(u32, UInt32Array);
fn_write_field!(u64, UInt64Array);
fn_write_field!(i8, Int8Array);
fn_write_field!(i16, Int16Array);
fn_write_field!(i32, Int32Array);
fn_write_field!(i64, Int64Array);
fn_write_field!(f32, Float32Array);
fn_write_field!(f64, Float64Array);

fn write_field_date32(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<Date32Array>().unwrap();
    to.set_date32(col_idx, from.value(row_idx));
}

fn write_field_date64(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<Date64Array>().unwrap();
    to.set_date64(col_idx, from.value(row_idx));
}

fn write_field_utf8(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<StringArray>().unwrap();
    let s = from.value(row_idx);
    let new_width = to.current_width() + s.as_bytes().len();
    if new_width > to.data.capacity() {
        // double the capacity to avoid repeated resize
        to.data.resize(max(to.data.capacity() * 2, new_width), 0);
    }
    to.set_utf8(col_idx, s);
}

fn write_field_binary(
    to: &mut RowWriter,
    from: &Arc<dyn Array>,
    col_idx: usize,
    row_idx: usize,
) {
    let from = from.as_any().downcast_ref::<BinaryArray>().unwrap();
    let s = from.value(row_idx);
    let new_width = to.current_width() + s.len();
    if new_width > to.data.capacity() {
        // double the capacity to avoid repeated resize
        to.data.resize(max(to.data.capacity() * 2, new_width), 0);
    }
    to.set_binary(col_idx, s);
}

fn write_field(
    col_idx: usize,
    row_idx: usize,
    col: &Arc<dyn Array>,
    dt: &DataType,
    row: &mut RowWriter,
) {
    use DataType::*;
    match dt {
        Boolean => write_field_bool(row, col, col_idx, row_idx),
        UInt8 => write_field_u8(row, col, col_idx, row_idx),
        UInt16 => write_field_u16(row, col, col_idx, row_idx),
        UInt32 => write_field_u32(row, col, col_idx, row_idx),
        UInt64 => write_field_u64(row, col, col_idx, row_idx),
        Int8 => write_field_i8(row, col, col_idx, row_idx),
        Int16 => write_field_i16(row, col, col_idx, row_idx),
        Int32 => write_field_i32(row, col, col_idx, row_idx),
        Int64 => write_field_i64(row, col, col_idx, row_idx),
        Float32 => write_field_f32(row, col, col_idx, row_idx),
        Float64 => write_field_f64(row, col, col_idx, row_idx),
        Date32 => write_field_date32(row, col, col_idx, row_idx),
        Date64 => write_field_date64(row, col, col_idx, row_idx),
        Utf8 => write_field_utf8(row, col, col_idx, row_idx),
        Binary => write_field_binary(row, col, col_idx, row_idx),
        _ => unimplemented!(),
    }
}
