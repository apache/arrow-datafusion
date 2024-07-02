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

//! [`ArrowBytesMap`] and [`ArrowBytesSet`] for storing maps/sets of values from
//! StringArray / LargeStringArray / BinaryArray / LargeBinaryArray.

use ahash::RandomState;
use arrow::array::cast::AsArray;
use arrow::array::types::{ByteArrayType, GenericBinaryType, GenericStringType};
use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, BufferBuilder, GenericBinaryArray,
    GenericStringArray, OffsetSizeTrait,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::DataType;
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::utils::proxy::{RawTableAllocExt, VecAllocExt};
use std::any::type_name;
use std::fmt::Debug;
use std::mem;
use std::ops::Range;
use std::sync::Arc;

/// Should the output be a String or Binary?
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputType {
    /// `StringArray` or `LargeStringArray`
    Utf8,
    /// `BinaryArray` or `LargeBinaryArray`
    Binary,
}

/// HashSet optimized for storing string or binary values that can produce that
/// the final set as a GenericStringArray with minimal copies.
#[derive(Debug)]
pub struct ArrowBytesSet<O: OffsetSizeTrait>(ArrowBytesMap<O, ()>);

impl<O: OffsetSizeTrait> ArrowBytesSet<O> {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesMap::new(output_type))
    }

    /// Return the contents of this set and replace it with a new empty
    /// set with the same output type
    pub(super) fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    /// Inserts each value from `values` into the set
    pub fn insert(&mut self, values: &ArrayRef) {
        fn make_payload_fn(_value: Option<&[u8]>) {}
        fn observe_payload_fn(_payload: ()) {}
        self.0
            .insert_if_new(values, make_payload_fn, observe_payload_fn);
    }

    /// Converts this set into a `StringArray`/`LargeStringArray` or
    /// `BinaryArray`/`LargeBinaryArray` containing each distinct value that
    /// was interned. This is done without copying the values.
    pub fn into_state(self) -> ArrayRef {
        self.0.into_state()
    }

    /// Returns the total number of distinct values (including nulls) seen so far
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// returns the total number of distinct values (not including nulls) seen so far
    pub fn non_null_len(&self) -> usize {
        self.0.non_null_len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        self.0.size()
    }
}

/// Optimized map for storing Arrow "bytes" types (`String`, `LargeString`,
/// `Binary`, and `LargeBinary`) values that can produce the set of keys on
/// output as `GenericBinaryArray` without copies.
///
/// Equivalent to `HashSet<String, V>` but with better performance for arrow
/// data.
///
/// # Generic Arguments
///
/// * `O`: OffsetSize (String/LargeString)
/// * `V`: payload type
///
/// # Description
///
/// This is a specialized HashMap with the following properties:
///
/// 1. Optimized for storing and emitting Arrow byte types  (e.g.
/// `StringArray` / `BinaryArray`) very efficiently by minimizing copying of
/// the string values themselves, both when inserting and when emitting the
/// final array.
///
///
/// 2. Retains the insertion order of entries in the final array. The values are
/// in the same order as they were inserted.
///
/// Note this structure can be used as a `HashSet` by specifying the value type
/// as `()`, as is done by [`ArrowBytesSet`].
///
/// This map is used by the special `COUNT DISTINCT` aggregate function to
/// store the distinct values, and by the `GROUP BY` operator to store
/// group values when they are a single string array.
///
/// # Example
///
/// The following diagram shows how the map would store the four strings
/// "Foo", NULL, "Bar", "TheQuickBrownFox":
///
/// * `hashtable` stores entries for each distinct string that has been
/// inserted. The entries contain the payload as well as information about the
/// value (either an offset or the actual bytes, see `Entry` docs for more
/// details)
///
/// * `offsets` stores offsets into `buffer` for each distinct string value,
/// following the same convention as the offsets in a `StringArray` or
/// `LargeStringArray`.
///
/// * `buffer` stores the actual byte data
///
/// * `null`: stores the index and payload of the null value, in this case the
/// second value (index 1)
///
/// ```text
/// ┌───────────────────────────────────┐    ┌─────┐    ┌────┐
/// │                ...                │    │  0  │    │FooB│
/// │ ┌──────────────────────────────┐  │    │  0  │    │arTh│
/// │ │      <Entry for "Bar">       │  │    │  3  │    │eQui│
/// │ │            len: 3            │  │    │  3  │    │ckBr│
/// │ │   offset_or_inline: "Bar"    │  │    │  6  │    │ownF│
/// │ │         payload:...          │  │    │     │    │ox  │
/// │ └──────────────────────────────┘  │    │     │    │    │
/// │                ...                │    └─────┘    └────┘
/// │ ┌──────────────────────────────┐  │
/// │ │<Entry for "TheQuickBrownFox">│  │    offsets    buffer
/// │ │           len: 16            │  │
/// │ │     offset_or_inline: 6      │  │    ┌───────────────┐
/// │ │         payload: ...         │  │    │    Some(1)    │
/// │ └──────────────────────────────┘  │    │ payload: ...  │
/// │                ...                │    └───────────────┘
/// └───────────────────────────────────┘
///                                              null
///               HashTable
/// ```
///
/// # Entry Format
///
/// Entries stored in a [`ArrowBytesMap`] represents a value that is either
/// stored inline or in the buffer
///
/// This helps the case where there are many short (less than 8 bytes) strings
/// that are the same (e.g. "MA", "CA", "NY", "TX", etc)
///
/// ```text
///                                                                ┌──────────────────┐
///                                                  ─ ─ ─ ─ ─ ─ ─▶│...               │
///                                                 │              │TheQuickBrownFox  │
///                                                                │...               │
///                                                 │              │                  │
///                                                                └──────────────────┘
///                                                 │               buffer of u8
///
///                                                 │
///                        ┌────────────────┬───────────────┬───────────────┐
///  Storing               │                │ starting byte │  length, in   │
///  "TheQuickBrownFox"    │   hash value   │   offset in   │  bytes (not   │
///  (long string)         │                │    buffer     │  characters)  │
///                        └────────────────┴───────────────┴───────────────┘
///                              8 bytes          8 bytes       4 or 8
///
///
///                         ┌───────────────┬─┬─┬─┬─┬─┬─┬─┬─┬───────────────┐
/// Storing "foobar"        │               │ │ │ │ │ │ │ │ │  length, in   │
/// (short string)          │  hash value   │?│?│f│o│o│b│a│r│  bytes (not   │
///                         │               │ │ │ │ │ │ │ │ │  characters)  │
///                         └───────────────┴─┴─┴─┴─┴─┴─┴─┴─┴───────────────┘
///                              8 bytes         8 bytes        4 or 8
/// ```
pub struct ArrowBytesMap<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// Should the output be String or Binary?
    output_type: OutputType,
    /// Underlying hash set for each distinct value
    map: hashbrown::raw::RawTable<Entry<O, V>>,
    /// Total size of the map in bytes
    map_size: usize,
    /// In progress arrow `Buffer` containing all values
    buffer: BufferBuilder<u8>,
    /// Offsets into `buffer` for each distinct  value. These offsets as used
    /// directly to create the final `GenericBinaryArray`. The `i`th string is
    /// stored in the range `offsets[i]..offsets[i+1]` in `buffer`. Null values
    /// are stored as a zero length string.
    offsets: Vec<O>,
    /// random state used to generate hashes
    random_state: RandomState,
    /// buffer that stores hash values (reused across batches to save allocations)
    hashes_buffer: Vec<u64>,
    /// `(payload, null_index)` for the 'null' value, if any
    /// NOTE null_index is the logical index in the final array, not the index
    /// in the buffer
    null: Option<(V, usize)>,
}

/// The size, in number of entries, of the initial hash table
const INITIAL_MAP_CAPACITY: usize = 128;
/// The initial size, in bytes, of the string data
const INITIAL_BUFFER_CAPACITY: usize = 8 * 1024;
impl<O: OffsetSizeTrait, V> ArrowBytesMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    pub fn new(output_type: OutputType) -> Self {
        Self {
            output_type,
            map: hashbrown::raw::RawTable::with_capacity(INITIAL_MAP_CAPACITY),
            map_size: 0,
            buffer: BufferBuilder::new(INITIAL_BUFFER_CAPACITY),
            offsets: vec![O::default()], // first offset is always 0
            random_state: RandomState::new(),
            hashes_buffer: vec![],
            null: None,
        }
    }

    /// Return the contents of this map and replace it with a new empty map with
    /// the same output type
    pub fn take(&mut self) -> Self {
        let mut new_self = Self::new(self.output_type);
        std::mem::swap(self, &mut new_self);
        new_self
    }

    /// Inserts each value from `values` into the map, invoking `payload_fn` for
    /// each value if *not* already present, deferring the allocation of the
    /// payload until it is needed.
    ///
    /// Note that this is different than a normal map that would replace the
    /// existing entry
    ///
    /// # Arguments:
    ///
    /// `values`: array whose values are inserted
    ///
    /// `make_payload_fn`:  invoked for each value that is not already present
    /// to create the payload, in order of the values in `values`
    ///
    /// `observe_payload_fn`: invoked once, for each value in `values`, that was
    /// already present in the map, with corresponding payload value.
    ///
    /// # Returns
    ///
    /// The payload value for the entry, either the existing value or
    /// the newly inserted value
    ///
    /// # Safety:
    ///
    /// Note that `make_payload_fn` and `observe_payload_fn` are only invoked
    /// with valid values from `values`, not for the `NULL` value.
    pub fn insert_if_new<MP, OP>(
        &mut self,
        values: &ArrayRef,
        make_payload_fn: MP,
        observe_payload_fn: OP,
    ) where
        MP: FnMut(Option<&[u8]>) -> V,
        OP: FnMut(V),
    {
        // Sanity array type
        match self.output_type {
            OutputType::Binary => {
                assert!(matches!(
                    values.data_type(),
                    DataType::Binary | DataType::LargeBinary
                ));
                self.insert_if_new_inner::<MP, OP, GenericBinaryType<O>>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
            }
            OutputType::Utf8 => {
                assert!(matches!(
                    values.data_type(),
                    DataType::Utf8 | DataType::LargeUtf8
                ));
                self.insert_if_new_inner::<MP, OP, GenericStringType<O>>(
                    values,
                    make_payload_fn,
                    observe_payload_fn,
                )
            }
        };
    }

    /// Generic version of [`Self::insert_if_new`] that handles `ByteArrayType`
    /// (both String and Binary)
    ///
    /// Note this is the only function that is generic on [`ByteArrayType`], which
    /// avoids having to template the entire structure,  making the code
    /// simpler and understand and reducing code bloat due to duplication.
    ///
    /// See comments on `insert_if_new` for more details
    fn insert_if_new_inner<MP, OP, B>(
        &mut self,
        values: &ArrayRef,
        mut make_payload_fn: MP,
        mut observe_payload_fn: OP,
    ) where
        MP: FnMut(Option<&[u8]>) -> V,
        OP: FnMut(V),
        B: ByteArrayType,
    {
        // step 1: compute hashes
        let batch_hashes = &mut self.hashes_buffer;
        batch_hashes.clear();
        batch_hashes.resize(values.len(), 0);
        create_hashes(&[Arc::clone(values)], &self.random_state, batch_hashes)
            // hash is supported for all types and create_hashes only
            // returns errors for unsupported types
            .unwrap();

        // step 2: insert each value into the set, if not already present
        let values = values.as_bytes::<B>();

        // Ensure lengths are equivalent
        assert_eq!(values.len(), batch_hashes.len());

        for (value, &hash) in values.iter().zip(batch_hashes.iter()) {
            // hande null value
            let Some(value) = value else {
                let payload = if let Some(&(payload, _offset)) = self.null.as_ref() {
                    payload
                } else {
                    let payload = make_payload_fn(None);
                    let null_index = self.offsets.len() - 1;
                    // nulls need a zero length in the offset buffer
                    let offset = self.buffer.len();
                    self.offsets.push(O::usize_as(offset));
                    self.null = Some((payload, null_index));
                    payload
                };
                observe_payload_fn(payload);
                continue;
            };

            // get the value as bytes
            let value: &[u8] = value.as_ref();
            let value_len = O::usize_as(value.len());

            // value is "small"
            let payload = if value.len() <= SHORT_VALUE_LEN {
                let inline = value.iter().fold(0usize, |acc, &x| acc << 8 | x as usize);

                // is value is already present in the set?
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value_len {
                        return false;
                    }
                    // value is stored inline so no need to consult buffer
                    // (this is the "small string optimization")
                    inline == header.offset_or_inline
                });

                if let Some(entry) = entry {
                    entry.payload
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it appears
                    // the output array, but store the actual bytes inline for
                    // comparison
                    self.buffer.append_slice(value);
                    self.offsets.push(O::usize_as(self.buffer.len()));
                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        len: value_len,
                        offset_or_inline: inline,
                        payload,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                    payload
                }
            }
            // value is not "small"
            else {
                // Check if the value is already present in the set
                let entry = self.map.get_mut(hash, |header| {
                    // compare value if hashes match
                    if header.len != value_len {
                        return false;
                    }
                    // Need to compare the bytes in the buffer
                    // SAFETY: buffer is only appended to, and we correctly inserted values and offsets
                    let existing_value =
                        unsafe { self.buffer.as_slice().get_unchecked(header.range()) };
                    value == existing_value
                });

                if let Some(entry) = entry {
                    entry.payload
                }
                // if no existing entry, make a new one
                else {
                    // Put the small values into buffer and offsets so it
                    // appears the output array, and store that offset
                    // so the bytes can be compared if needed
                    let offset = self.buffer.len(); // offset of start fof data
                    self.buffer.append_slice(value);
                    self.offsets.push(O::usize_as(self.buffer.len()));

                    let payload = make_payload_fn(Some(value));
                    let new_header = Entry {
                        hash,
                        len: value_len,
                        offset_or_inline: offset,
                        payload,
                    };
                    self.map.insert_accounted(
                        new_header,
                        |header| header.hash,
                        &mut self.map_size,
                    );
                    payload
                }
            };
            observe_payload_fn(payload);
        }
        // Check for overflow in offsets (if more data was sent than can be represented)
        if O::from_usize(self.buffer.len()).is_none() {
            panic!(
                "Put {} bytes in buffer, more than can be represented by a {}",
                self.buffer.len(),
                type_name::<O>()
            );
        }
    }

    /// Converts this set into a `StringArray`, `LargeStringArray`,
    /// `BinaryArray`, or `LargeBinaryArray` containing each distinct value
    /// that was inserted. This is done without copying the values.
    ///
    /// The values are guaranteed to be returned in the same order in which
    /// they were first seen.
    pub fn into_state(self) -> ArrayRef {
        let Self {
            output_type,
            map: _,
            map_size: _,
            offsets,
            mut buffer,
            random_state: _,
            hashes_buffer: _,
            null,
        } = self;

        // Only make a `NullBuffer` if there was a null value
        let nulls = null.map(|(_payload, null_index)| {
            let num_values = offsets.len() - 1;
            single_null_buffer(num_values, null_index)
        });
        // SAFETY: the offsets were constructed correctly in `insert_if_new` --
        // monotonically increasing, overflows were checked.
        let offsets = unsafe { OffsetBuffer::new_unchecked(ScalarBuffer::from(offsets)) };
        let values = buffer.finish();

        match output_type {
            OutputType::Binary => {
                // SAFETY: the offsets were constructed correctly
                Arc::new(unsafe {
                    GenericBinaryArray::new_unchecked(offsets, values, nulls)
                })
            }
            OutputType::Utf8 => {
                // SAFETY:
                // 1. the offsets were constructed safely
                //
                // 2. we asserted the input arrays were all the correct type and
                // thus since all the values that went in were valid (e.g. utf8)
                // so are all the values that come out
                Arc::new(unsafe {
                    GenericStringArray::new_unchecked(offsets, values, nulls)
                })
            }
        }
    }

    /// Total number of entries (including null, if present)
    pub fn len(&self) -> usize {
        self.non_null_len() + self.null.map(|_| 1).unwrap_or(0)
    }

    /// Is the set empty?
    pub fn is_empty(&self) -> bool {
        self.map.is_empty() && self.null.is_none()
    }

    /// Number of non null entries
    pub fn non_null_len(&self) -> usize {
        self.map.len()
    }

    /// Return the total size, in bytes, of memory used to store the data in
    /// this set, not including `self`
    pub fn size(&self) -> usize {
        self.map_size
            + self.buffer.capacity() * std::mem::size_of::<u8>()
            + self.offsets.allocated_size()
            + self.hashes_buffer.allocated_size()
    }
}

/// Returns a `NullBuffer` with a single null value at the given index
fn single_null_buffer(num_values: usize, null_index: usize) -> NullBuffer {
    let mut bool_builder = BooleanBufferBuilder::new(num_values);
    bool_builder.append_n(num_values, true);
    bool_builder.set_bit(null_index, false);
    NullBuffer::from(bool_builder.finish())
}

impl<O: OffsetSizeTrait, V> Debug for ArrowBytesMap<O, V>
where
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrowBytesMap")
            .field("map", &"<map>")
            .field("map_size", &self.map_size)
            .field("buffer", &self.buffer)
            .field("random_state", &self.random_state)
            .field("hashes_buffer", &self.hashes_buffer)
            .finish()
    }
}

/// Maximum size of a value that can be inlined in the hash table
const SHORT_VALUE_LEN: usize = mem::size_of::<usize>();

/// Entry in the hash table -- see [`ArrowBytesMap`] for more details
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// hash of the value (stored to avoid recomputing it in hash table check)
    hash: u64,
    /// if len =< [`SHORT_VALUE_LEN`]: the data inlined
    /// if len > [`SHORT_VALUE_LEN`], the offset of where the data starts
    offset_or_inline: usize,
    /// length of the value, in bytes (use O here so we use only i32 for
    /// strings, rather 64 bit usize)
    len: O,
    /// value stored by the entry
    payload: V,
}

impl<O, V> Entry<O, V>
where
    O: OffsetSizeTrait,
    V: Debug + PartialEq + Eq + Clone + Copy + Default,
{
    /// returns self.offset..self.offset + self.len
    #[inline(always)]
    fn range(&self) -> Range<usize> {
        self.offset_or_inline..self.offset_or_inline + self.len.as_usize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, LargeBinaryArray, StringArray};
    use std::collections::HashMap;

    #[test]
    fn string_set_empty() {
        let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
        let array: ArrayRef = Arc::new(StringArray::new_null(0));
        set.insert(&array);
        assert_eq!(set.len(), 0);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[]);
    }

    #[test]
    fn string_set_one_null() {
        let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
        let array: ArrayRef = Arc::new(StringArray::new_null(1));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn string_set_many_null() {
        let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
        let array: ArrayRef = Arc::new(StringArray::new_null(11));
        set.insert(&array);
        assert_eq!(set.len(), 1);
        assert_eq!(set.non_null_len(), 0);
        assert_set(set, &[None]);
    }

    #[test]
    fn string_set_basic_i32() {
        test_string_set_basic::<i32>();
    }

    #[test]
    fn string_set_basic_i64() {
        test_string_set_basic::<i64>();
    }

    fn test_string_set_basic<O: OffsetSizeTrait>() {
        // basic test for mixed small and large string values
        let values = GenericStringArray::<O>::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCC"), // 10 bytes
            Some(""),
            Some("cbcxx"), // 5 bytes
            None,
            Some("AAAAAAAA"),  // 8 bytes
            Some("BBBBBQBBB"), // 9 bytes
            Some("a"),
            Some("cbcxx"),
            Some("b"),
            Some("cbcxx"),
            Some(""),
            None,
            Some("BBBBBQBBB"),
            Some("BBBBBQBBB"),
            Some("AAAAAAAA"),
            Some("CXCCCCCCCC"),
        ]);

        let mut set = ArrowBytesSet::<O>::new(OutputType::Utf8);
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // values mut appear be in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("b"),
                Some("CXCCCCCCCC"),
                Some(""),
                Some("cbcxx"),
                None,
                Some("AAAAAAAA"),
                Some("BBBBBQBBB"),
            ],
        );
    }

    #[test]
    fn string_set_non_utf8_32() {
        test_string_set_non_utf8::<i32>();
    }

    #[test]
    fn string_set_non_utf8_64() {
        test_string_set_non_utf8::<i64>();
    }

    fn test_string_set_non_utf8<O: OffsetSizeTrait>() {
        // basic test for mixed small and large string values
        let values = GenericStringArray::<O>::from(vec![
            Some("a"),
            Some("✨🔥"),
            Some("🔥"),
            Some("✨✨✨"),
            Some("foobarbaz"),
            Some("🔥"),
            Some("✨🔥"),
        ]);

        let mut set = ArrowBytesSet::<O>::new(OutputType::Utf8);
        let array: ArrayRef = Arc::new(values);
        set.insert(&array);
        // strings mut appear be in the order they were inserted
        assert_set(
            set,
            &[
                Some("a"),
                Some("✨🔥"),
                Some("🔥"),
                Some("✨✨✨"),
                Some("foobarbaz"),
            ],
        );
    }

    // asserts that the set contains the expected strings, in the same order
    fn assert_set<O: OffsetSizeTrait>(set: ArrowBytesSet<O>, expected: &[Option<&str>]) {
        let strings = set.into_state();
        let strings = strings.as_string::<O>();
        let state = strings.into_iter().collect::<Vec<_>>();
        assert_eq!(state, expected);
    }

    // Test use of binary output type
    #[test]
    fn test_binary_set() {
        let values: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"a"),
            Some(b"CXCCCCCCCC"),
            None,
            Some(b"CXCCCCCCCC"),
        ]));

        let expected: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![
            Some(b"a"),
            Some(b"CXCCCCCCCC"),
            None,
        ]));

        let mut set = ArrowBytesSet::<i32>::new(OutputType::Binary);
        set.insert(&values);
        assert_eq!(&set.into_state(), &expected);
    }

    // Test use of binary output type
    #[test]
    fn test_large_binary_set() {
        let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![
            Some(b"a"),
            Some(b"CXCCCCCCCC"),
            None,
            Some(b"CXCCCCCCCC"),
        ]));

        let expected: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![
            Some(b"a"),
            Some(b"CXCCCCCCCC"),
            None,
        ]));

        let mut set = ArrowBytesSet::<i64>::new(OutputType::Binary);
        set.insert(&values);
        assert_eq!(&set.into_state(), &expected);
    }

    #[test]
    #[should_panic(
        expected = "matches!(values.data_type(), DataType::Utf8 | DataType::LargeUtf8)"
    )]
    fn test_mismatched_types() {
        // inserting binary into a set that expects strings should panic
        let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(b"a")]));

        let mut set = ArrowBytesSet::<i64>::new(OutputType::Utf8);
        set.insert(&values);
    }

    #[test]
    #[should_panic]
    fn test_mismatched_sizes() {
        // inserting large strings into a set that expects small should panic
        let values: ArrayRef = Arc::new(LargeBinaryArray::from_opt_vec(vec![Some(b"a")]));

        let mut set = ArrowBytesSet::<i32>::new(OutputType::Binary);
        set.insert(&values);
    }

    // put more than 2GB in a string set and expect it to panic
    #[test]
    #[should_panic(
        expected = "Put 2147483648 bytes in buffer, more than can be represented by a i32"
    )]
    fn test_string_overflow() {
        let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
        for value in ["a", "b", "c"] {
            // 1GB strings, so 3rd is over 2GB and should panic
            let arr: ArrayRef =
                Arc::new(StringArray::from_iter_values([value.repeat(1 << 30)]));
            set.insert(&arr);
        }
    }

    // inserting strings into the set does not increase reported memory
    #[test]
    fn test_string_set_memory_usage() {
        let strings1 = GenericStringArray::<i32>::from(vec![
            Some("a"),
            Some("b"),
            Some("CXCCCCCCCC"), // 10 bytes
            Some("AAAAAAAA"),   // 8 bytes
            Some("BBBBBQBBB"),  // 9 bytes
        ]);
        let total_strings1_len = strings1
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values1: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings1));

        // Much larger strings in strings2
        let strings2 = GenericStringArray::<i32>::from(vec![
            "FOO".repeat(1000),
            "BAR".repeat(2000),
            "BAZ".repeat(3000),
        ]);
        let total_strings2_len = strings2
            .iter()
            .map(|s| s.map(|s| s.len()).unwrap_or(0))
            .sum::<usize>();
        let values2: ArrayRef = Arc::new(GenericStringArray::<i32>::from(strings2));

        let mut set = ArrowBytesSet::<i32>::new(OutputType::Utf8);
        let size_empty = set.size();

        set.insert(&values1);
        let size_after_values1 = set.size();
        assert!(size_empty < size_after_values1);
        assert!(
            size_after_values1 > total_strings1_len,
            "expect {size_after_values1} to be more than {total_strings1_len}"
        );
        assert!(size_after_values1 < total_strings1_len + total_strings2_len);

        // inserting the same strings should not affect the size
        set.insert(&values1);
        assert_eq!(set.size(), size_after_values1);

        // inserting the large strings should increase the reported size
        set.insert(&values2);
        let size_after_values2 = set.size();
        assert!(size_after_values2 > size_after_values1);
        assert!(size_after_values2 > total_strings1_len + total_strings2_len);
    }

    #[test]
    fn test_map() {
        let input = vec![
            // Note mix of short/long strings
            Some("A"),
            Some("bcdefghijklmnop"),
            Some("X"),
            Some("Y"),
            None,
            Some("qrstuvqxyzhjwya"),
            Some("✨🔥"),
            Some("🔥"),
            Some("🔥🔥🔥🔥🔥🔥"),
        ];

        let mut test_map = TestMap::new();
        test_map.insert(&input);
        test_map.insert(&input); // put it in twice
        let expected_output: ArrayRef = Arc::new(StringArray::from(input));
        assert_eq!(&test_map.into_array(), &expected_output);
    }

    #[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
    struct TestPayload {
        // store the string value to check against input
        index: usize, // store the index of the string (each new string gets the next sequential input)
    }

    /// Wraps an [`ArrowBytesMap`], validating its invariants
    struct TestMap {
        map: ArrowBytesMap<i32, TestPayload>,
        // stores distinct strings seen, in order
        strings: Vec<Option<String>>,
        // map strings to index in strings
        indexes: HashMap<Option<String>, usize>,
    }

    impl Debug for TestMap {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestMap")
                .field("map", &"...")
                .field("strings", &self.strings)
                .field("indexes", &self.indexes)
                .finish()
        }
    }

    impl TestMap {
        /// creates a map with TestPayloads for the given strings and then
        /// validates the payloads
        fn new() -> Self {
            Self {
                map: ArrowBytesMap::new(OutputType::Utf8),
                strings: vec![],
                indexes: HashMap::new(),
            }
        }

        /// Inserts strings into the map
        fn insert(&mut self, strings: &[Option<&str>]) {
            let string_array = StringArray::from(strings.to_vec());
            let arr: ArrayRef = Arc::new(string_array);

            let mut next_index = self.indexes.len();
            let mut actual_new_strings = vec![];
            let mut actual_seen_indexes = vec![];
            // update self with new values, keeping track of newly added values
            for str in strings {
                let str = str.map(|s| s.to_string());
                let index = self.indexes.get(&str).cloned().unwrap_or_else(|| {
                    actual_new_strings.push(str.clone());
                    let index = self.strings.len();
                    self.strings.push(str.clone());
                    self.indexes.insert(str, index);
                    index
                });
                actual_seen_indexes.push(index);
            }

            // insert the values into the map, recording what we did
            let mut seen_new_strings = vec![];
            let mut seen_indexes = vec![];
            self.map.insert_if_new(
                &arr,
                |s| {
                    let value = s
                        .map(|s| String::from_utf8(s.to_vec()).expect("Non utf8 string"));
                    let index = next_index;
                    next_index += 1;
                    seen_new_strings.push(value);
                    TestPayload { index }
                },
                |payload| {
                    seen_indexes.push(payload.index);
                },
            );

            assert_eq!(actual_seen_indexes, seen_indexes);
            assert_eq!(actual_new_strings, seen_new_strings);
        }

        /// Call `self.map.into_array()` validating that the strings are in the same
        /// order as they were inserted
        fn into_array(self) -> ArrayRef {
            let Self {
                map,
                strings,
                indexes: _,
            } = self;

            let arr = map.into_state();
            let expected: ArrayRef = Arc::new(StringArray::from(strings));
            assert_eq!(&arr, &expected);
            arr
        }
    }
}
