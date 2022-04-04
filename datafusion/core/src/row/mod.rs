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

//! An implementation of Row backed by raw bytes
//!
//! Each tuple consists of up to three parts: "`null bit set`" , "`values`"  and  "`var length data`"
//!
//! The null bit set is used for null tracking and is aligned to 1-byte. It stores
//! one bit per field.
//!
//! In the region of the values, we store the fields in the order they are defined in the schema.
//! - For fixed-length, sequential access fields, we store them directly.
//!       E.g., 4 bytes for int and 1 byte for bool.
//! - For fixed-length, update often fields, we store one 8-byte word per field.
//! - For fields of non-primitive or variable-length types,
//!       we append their actual content to the end of the var length region and
//!       store their offset relative to row base and their length, packed into an 8-byte word.
//!
//! ┌────────────────┬──────────────────────────┬───────────────────────┐        ┌───────────────────────┬────────────┐
//! │Validity Bitmask│    Fixed Width Field     │ Variable Width Field  │   ...  │     vardata area      │  padding   │
//! │ (byte aligned) │   (native type width)    │(vardata offset + len) │        │   (variable length)   │   bytes    │
//! └────────────────┴──────────────────────────┴───────────────────────┘        └───────────────────────┴────────────┘
//!
//!  For example, given the schema (Int8, Utf8, Float32, Utf8)
//!
//!  Encoding the tuple (1, "FooBar", NULL, "baz")
//!
//!  Requires 32 bytes (31 bytes payload and 1 byte padding to make each tuple 8-bytes aligned):
//!
//! ┌──────────┬──────────┬──────────────────────┬──────────────┬──────────────────────┬───────────────────────┬──────────┐
//! │0b00001011│   0x01   │0x00000016  0x00000006│  0x00000000  │0x0000001C  0x00000003│       FooBarbaz       │   0x00   │
//! └──────────┴──────────┴──────────────────────┴──────────────┴──────────────────────┴───────────────────────┴──────────┘
//! 0          1          2                     10              14                     22                     31         32
//!

use arrow::datatypes::{DataType, Schema};
use arrow::util::bit_util::{ceil, get_bit_raw, round_upto_power_of_2};
use std::fmt::Write;
use std::sync::Arc;

pub mod reader;
pub mod writer;

const ALL_VALID_MASK: [u8; 8] = [1, 3, 7, 15, 31, 63, 127, 255];

const UTF8_DEFAULT_SIZE: usize = 20;
const BINARY_DEFAULT_SIZE: usize = 100;

/// Returns if all fields are valid
pub fn all_valid(data: &[u8], n: usize) -> bool {
    for item in data.iter().take(n / 8) {
        if *item != ALL_VALID_MASK[7] {
            return false;
        }
    }
    if n % 8 == 0 {
        true
    } else {
        data[n / 8] == ALL_VALID_MASK[n % 8 - 1]
    }
}

/// Show null bit for each field in a tuple, 1 for valid and 0 for null.
/// For a tuple with nine total fields, valid at field 0, 6, 7, 8 shows as `[10000011, 1]`.
pub struct NullBitsFormatter<'a> {
    null_bits: &'a [u8],
    field_count: usize,
}

impl<'a> NullBitsFormatter<'a> {
    /// new
    pub fn new(null_bits: &'a [u8], field_count: usize) -> Self {
        Self {
            null_bits,
            field_count,
        }
    }
}

impl<'a> std::fmt::Debug for NullBitsFormatter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut is_first = true;
        let data = self.null_bits;
        for i in 0..self.field_count {
            if is_first {
                f.write_char('[')?;
                is_first = false;
            } else if i % 8 == 0 {
                f.write_str(", ")?;
            }
            if unsafe { get_bit_raw(data.as_ptr(), i) } {
                f.write_char('1')?;
            } else {
                f.write_char('0')?;
            }
        }
        f.write_char(']')?;
        Ok(())
    }
}

/// Get relative offsets for each field and total width for values
fn get_offsets(null_width: usize, schema: &Arc<Schema>) -> (Vec<usize>, usize) {
    let mut offsets = vec![];
    let mut offset = null_width;
    for f in schema.fields() {
        offsets.push(offset);
        offset += type_width(f.data_type());
    }
    (offsets, offset - null_width)
}

fn supported_type(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Boolean
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float32
            | Float64
            | Date32
            | Date64
            | Utf8
            | Binary
    )
}

fn var_length(dt: &DataType) -> bool {
    use DataType::*;
    matches!(dt, Utf8 | Binary)
}

fn type_width(dt: &DataType) -> usize {
    use DataType::*;
    if var_length(dt) {
        return std::mem::size_of::<u64>();
    }
    match dt {
        Boolean | UInt8 | Int8 => 1,
        UInt16 | Int16 => 2,
        UInt32 | Int32 | Float32 | Date32 => 4,
        UInt64 | Int64 | Float64 | Date64 => 8,
        _ => unreachable!(),
    }
}

/// Estimate row width based on schema
pub fn estimate_row_width(schema: &Arc<Schema>) -> usize {
    let null_free = schema_null_free(schema);
    let field_count = schema.fields().len();
    let mut width = if null_free { 0 } else { ceil(field_count, 8) };
    for f in schema.fields() {
        width += type_width(f.data_type());
        match f.data_type() {
            DataType::Utf8 => width += UTF8_DEFAULT_SIZE,
            DataType::Binary => width += BINARY_DEFAULT_SIZE,
            _ => {}
        }
    }
    round_upto_power_of_2(width, 8)
}

/// Tell if the row is of fixed size
pub fn fixed_size(schema: &Arc<Schema>) -> bool {
    schema.fields().iter().all(|f| !var_length(f.data_type()))
}

/// Tell if we can create raw-bytes based rows since we currently
/// has limited data type supports in the row format
pub fn row_supported(schema: &Arc<Schema>) -> bool {
    schema
        .fields()
        .iter()
        .all(|f| supported_type(f.data_type()))
}

#[cfg(feature = "jit")]
#[macro_export]
/// register external functions to the assembler
macro_rules! reg_fn {
    ($ASS:ident, $FN: path, $PARAM: expr, $RET: expr) => {
        $ASS.register_extern_fn(fn_name($FN), $FN as *const u8, $PARAM, $RET)?;
    };
}

#[cfg(feature = "jit")]
fn fn_name<T>(f: T) -> &'static str {
    fn type_name_of<T>(_: T) -> &'static str {
        std::any::type_name::<T>()
    }
    let name = type_name_of(f);

    // Find and cut the rest of the path
    match &name.rfind(':') {
        Some(pos) => &name[pos + 1..name.len()],
        None => name,
    }
}

/// Tell if schema contains no nullable field
pub fn schema_null_free(schema: &Arc<Schema>) -> bool {
    schema.fields().iter().all(|f| !f.is_nullable())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::file_format::parquet::ParquetFormat;
    use crate::datasource::file_format::FileFormat;
    use crate::datasource::listing::local_unpartitioned_file;
    use crate::error::Result;
    use crate::physical_plan::file_format::FileScanConfig;
    use crate::physical_plan::{collect, ExecutionPlan};
    use crate::prelude::SessionContext;
    use crate::row::reader::read_as_batch;
    #[cfg(feature = "jit")]
    use crate::row::reader::read_as_batch_jit;
    use crate::row::writer::write_batch_unchecked;
    #[cfg(feature = "jit")]
    use crate::row::writer::write_batch_unchecked_jit;
    use arrow::record_batch::RecordBatch;
    use arrow::util::bit_util::{ceil, set_bit_raw, unset_bit_raw};
    use arrow::{array::*, datatypes::*};
    use datafusion_data_access::object_store::local::LocalFileSystem;
    use datafusion_data_access::object_store::local::{
        local_object_reader, local_object_reader_stream,
    };
    #[cfg(feature = "jit")]
    use datafusion_jit::api::Assembler;
    use rand::Rng;
    use DataType::*;

    fn test_validity(bs: &[bool]) {
        let n = bs.len();
        let mut data = vec![0; ceil(n, 8)];
        for (i, b) in bs.iter().enumerate() {
            if *b {
                let data_argument = &mut data;
                unsafe {
                    set_bit_raw(data_argument.as_mut_ptr(), i);
                };
            } else {
                let data_argument = &mut data;
                unsafe {
                    unset_bit_raw(data_argument.as_mut_ptr(), i);
                };
            }
        }
        let expected = bs.iter().all(|f| *f);
        assert_eq!(all_valid(&data, bs.len()), expected);
    }

    #[test]
    fn test_all_valid() {
        let sizes = [4, 8, 12, 16, 19, 23, 32, 44];
        for i in sizes {
            {
                // contains false
                let input = {
                    let mut rng = rand::thread_rng();
                    let mut input: Vec<bool> = vec![false; i];
                    rng.fill(&mut input[..]);
                    input
                };
                test_validity(&input);
            }

            {
                // all true
                let input = vec![true; i];
                test_validity(&input);
            }
        }
    }

    #[test]
    fn test_formatter() -> std::fmt::Result {
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001], 8)),
            "[10000011]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1], 9)),
            "[10000011, 1]"
        );
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 2)), "[10]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 3)), "[100]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 4)), "[1000]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 5)), "[10000]");
        assert_eq!(format!("{:?}", NullBitsFormatter::new(&[1], 6)), "[100000]");
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[1], 7)),
            "[1000000]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[1], 8)),
            "[10000000]"
        );
        // extra bytes are ignored
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1, 1, 1], 9)),
            "[10000011, 1]"
        );
        assert_eq!(
            format!("{:?}", NullBitsFormatter::new(&[0b11000001, 1, 1], 16)),
            "[10000011, 10000000]"
        );
        Ok(())
    }

    macro_rules! fn_test_single_type {
        ($ARRAY: ident, $TYPE: expr, $VEC: expr) => {
            paste::item! {
                #[test]
                #[allow(non_snake_case)]
                fn [<test_single_ $TYPE>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, true)]));
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let row_offsets =
                        { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
                    let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }

                #[test]
                #[allow(non_snake_case)]
                #[cfg(feature = "jit")]
                fn [<test_single_ $TYPE _jit>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, true)]));
                    let a = $ARRAY::from($VEC);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let assembler = Assembler::default();
                    let row_offsets =
                        { write_batch_unchecked_jit(&mut vector, 0, &batch, 0, schema.clone(), &assembler)? };
                    let output_batch = { read_as_batch_jit(&vector, schema, &row_offsets, &assembler)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }

                #[test]
                #[allow(non_snake_case)]
                fn [<test_single_ $TYPE _null_free>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, false)]));
                    let v = $VEC.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
                    let a = $ARRAY::from(v);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let row_offsets =
                        { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
                    let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }

                #[test]
                #[allow(non_snake_case)]
                #[cfg(feature = "jit")]
                fn [<test_single_ $TYPE _jit_null_free>]() -> Result<()> {
                    let schema = Arc::new(Schema::new(vec![Field::new("a", $TYPE, false)]));
                    let v = $VEC.into_iter().filter(|o| o.is_some()).collect::<Vec<_>>();
                    let a = $ARRAY::from(v);
                    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
                    let mut vector = vec![0; 1024];
                    let assembler = Assembler::default();
                    let row_offsets =
                        { write_batch_unchecked_jit(&mut vector, 0, &batch, 0, schema.clone(), &assembler)? };
                    let output_batch = { read_as_batch_jit(&vector, schema, &row_offsets, &assembler)? };
                    assert_eq!(batch, output_batch);
                    Ok(())
                }
            }
        };
    }

    fn_test_single_type!(
        BooleanArray,
        Boolean,
        vec![Some(true), Some(false), None, Some(true), None]
    );

    fn_test_single_type!(
        Int8Array,
        Int8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int16Array,
        Int16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int32Array,
        Int32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Int64Array,
        Int64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt8Array,
        UInt8,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt16Array,
        UInt16,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt32Array,
        UInt32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        UInt64Array,
        UInt64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Float32Array,
        Float32,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Float64Array,
        Float64,
        vec![Some(5.0), Some(7.0), None, Some(0.0), Some(111.0)]
    );

    fn_test_single_type!(
        Date32Array,
        Date32,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        Date64Array,
        Date64,
        vec![Some(5), Some(7), None, Some(0), Some(111)]
    );

    fn_test_single_type!(
        StringArray,
        Utf8,
        vec![Some("hello"), Some("world"), None, Some(""), Some("")]
    );

    #[test]
    fn test_single_binary() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, true)]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let a = BinaryArray::from_opt_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jit")]
    fn test_single_binary_jit() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, true)]));
        let values: Vec<Option<&[u8]>> =
            vec![Some(b"one"), Some(b"two"), None, Some(b""), Some(b"three")];
        let a = BinaryArray::from_opt_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let assembler = Assembler::default();
        let row_offsets = {
            write_batch_unchecked_jit(
                &mut vector,
                0,
                &batch,
                0,
                schema.clone(),
                &assembler,
            )?
        };
        let output_batch =
            { read_as_batch_jit(&vector, schema, &row_offsets, &assembler)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    fn test_single_binary_null_free() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, false)]));
        let values: Vec<&[u8]> = vec![b"one", b"two", b"", b"three"];
        let a = BinaryArray::from_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, &batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[test]
    #[cfg(feature = "jit")]
    fn test_single_binary_jit_null_free() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", Binary, false)]));
        let values: Vec<&[u8]> = vec![b"one", b"two", b"", b"three"];
        let a = BinaryArray::from_vec(values);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(a)])?;
        let mut vector = vec![0; 8192];
        let assembler = Assembler::default();
        let row_offsets = {
            write_batch_unchecked_jit(
                &mut vector,
                0,
                &batch,
                0,
                schema.clone(),
                &assembler,
            )?
        };
        let output_batch =
            { read_as_batch_jit(&vector, schema, &row_offsets, &assembler)? };
        assert_eq!(batch, output_batch);
        Ok(())
    }

    #[tokio::test]
    async fn test_with_parquet() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let projection = Some(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let exec = get_exec("alltypes_plain.parquet", &projection, None).await?;
        let schema = exec.schema().clone();

        let batches = collect(exec, task_ctx).await?;
        assert_eq!(1, batches.len());
        let batch = &batches[0];

        let mut vector = vec![0; 20480];
        let row_offsets =
            { write_batch_unchecked(&mut vector, 0, batch, 0, schema.clone()) };
        let output_batch = { read_as_batch(&vector, schema, &row_offsets)? };
        assert_eq!(*batch, output_batch);

        Ok(())
    }

    #[test]
    #[should_panic(expected = "supported(schema)")]
    fn test_unsupported_type_write() {
        let a: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let schema = batch.schema();
        let mut vector = vec![0; 1024];
        write_batch_unchecked(&mut vector, 0, &batch, 0, schema);
    }

    #[test]
    #[should_panic(expected = "supported(schema)")]
    fn test_unsupported_type_read() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::Decimal(5, 2),
            false,
        )]));
        let vector = vec![0; 1024];
        let row_offsets = vec![0];
        read_as_batch(&vector, schema, &row_offsets).unwrap();
    }

    async fn get_exec(
        file_name: &str,
        projection: &Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let testdata = crate::test_util::parquet_test_data();
        let filename = format!("{}/{}", testdata, file_name);
        let format = ParquetFormat::default();
        let file_schema = format
            .infer_schema(local_object_reader_stream(vec![filename.clone()]))
            .await
            .expect("Schema inference");
        let statistics = format
            .infer_stats(local_object_reader(filename.clone()))
            .await
            .expect("Stats inference");
        let file_groups = vec![vec![local_unpartitioned_file(filename.clone())]];
        let exec = format
            .create_physical_plan(
                FileScanConfig {
                    object_store: Arc::new(LocalFileSystem {}),
                    file_schema,
                    file_groups,
                    statistics,
                    projection: projection.clone(),
                    limit,
                    table_partition_cols: vec![],
                },
                &[],
            )
            .await?;
        Ok(exec)
    }
}
