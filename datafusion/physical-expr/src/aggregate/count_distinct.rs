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

use arrow::datatypes::{ArrowDictionaryKeyType, DataType, Field};
use datafusion_common::cast::as_dictionary_array;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

type DistinctScalarValues = ScalarValue;

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCount {
    /// Column name
    name: String,
    /// The DataType used to hold the state for each input
    state_data_type: DataType,
    /// The input arguments
    expr: Arc<dyn PhysicalExpr>,
}

impl DistinctCount {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_type: DataType,
        expr: Arc<dyn PhysicalExpr>,
        name: String,
    ) -> Self {
        Self {
            name,
            state_data_type: input_data_type,
            expr,
        }
    }
}

impl AggregateExpr for DistinctCount {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::Int64, true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "count distinct"),
            DataType::List(Box::new(Field::new(
                "item",
                self.state_data_type.clone(),
                true,
            ))),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        use arrow::datatypes;
        use datatypes::DataType::*;

        Ok(match &self.state_data_type {
            Dictionary(key, _) if key.is_dictionary_key_type() => {
                match **key {
                    Int8 => Box::new(
                        CountDistinctDictAccumulator::<datatypes::Int8Type>::new(),
                    ),
                    Int16 => Box::new(
                        CountDistinctDictAccumulator::<datatypes::Int16Type>::new(),
                    ),
                    Int32 => Box::new(
                        CountDistinctDictAccumulator::<datatypes::Int32Type>::new(),
                    ),
                    Int64 => Box::new(
                        CountDistinctDictAccumulator::<datatypes::Int64Type>::new(),
                    ),
                    UInt8 => Box::new(
                        CountDistinctDictAccumulator::<datatypes::UInt8Type>::new(),
                    ),
                    UInt16 => Box::new(CountDistinctDictAccumulator::<
                        datatypes::UInt16Type,
                    >::new()),
                    UInt32 => Box::new(CountDistinctDictAccumulator::<
                        datatypes::UInt32Type,
                    >::new()),
                    UInt64 => Box::new(CountDistinctDictAccumulator::<
                        datatypes::UInt64Type,
                    >::new()),
                    _ => {
                        // just checked that datatype is a valid dict key type
                        unreachable!()
                    }
                }
            }
            _ => Box::new(DistinctCountAccumulator {
                values: HashSet::default(),
                state_data_type: self.state_data_type.clone(),
            }),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<DistinctScalarValues, RandomState>,
    state_data_type: DataType,
}

impl DistinctCountAccumulator {
    // calculating the size for fixed length values, taking first batch size * number of batches
    // This method is faster than .full_size(), however it is not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .next()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .unwrap_or(0)
            + std::mem::size_of::<DataType>()
    }

    // calculates the size as accurate as possible, call to this method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .sum::<usize>()
            + std::mem::size_of::<DataType>()
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut cols_out =
            ScalarValue::new_list(Some(Vec::new()), self.state_data_type.clone());
        self.values
            .iter()
            .enumerate()
            .for_each(|(_, distinct_values)| {
                if let ScalarValue::List(Some(ref mut v), _) = cols_out {
                    v.push(distinct_values.clone());
                }
            });
        Ok(vec![cols_out])
    }
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                self.values.insert(scalar);
            }
            Ok(())
        })
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let scalar = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::List(Some(scalar), _) = scalar {
                scalar.iter().for_each(|scalar| {
                    if !ScalarValue::is_null(scalar) {
                        self.values.insert(scalar.clone());
                    }
                });
            } else {
                return Err(DataFusionError::Internal(
                    "Unexpected accumulator state".into(),
                ));
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        match &self.state_data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }
}
/// Special case accumulator for counting distinct values in a dict
struct CountDistinctDictAccumulator<K>
where
    K: ArrowDictionaryKeyType + std::marker::Send + std::marker::Sync,
{
    /// `K` is required when casting to dict array
    _dt: core::marker::PhantomData<K>,
    /// laziliy initialized state that holds a boolean for each index.
    /// the bool at each index indicates whether the value for that index has been seen yet.
    state: Option<Vec<bool>>,
}

impl<K> std::fmt::Debug for CountDistinctDictAccumulator<K>
where
    K: ArrowDictionaryKeyType + std::marker::Send + std::marker::Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CountDistinctDictAccumulator")
            .field("state", &self.state)
            .finish()
    }
}
impl<K: ArrowDictionaryKeyType + std::marker::Send + std::marker::Sync>
    CountDistinctDictAccumulator<K>
{
    fn new() -> Self {
        Self {
            _dt: core::marker::PhantomData,
            state: None,
        }
    }
}
impl<K> Accumulator for CountDistinctDictAccumulator<K>
where
    K: ArrowDictionaryKeyType + std::marker::Send + std::marker::Sync,
{
    fn state(&self) -> Result<Vec<ScalarValue>> {
        if let Some(state) = &self.state {
            let bools = state
                .iter()
                .map(|b| ScalarValue::Boolean(Some(*b)))
                .collect();
            Ok(vec![ScalarValue::List(
                Some(bools),
                Box::new(Field::new("item", DataType::Boolean, false)),
            )])
        } else {
            // empty state
            Ok(vec![])
        }
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = as_dictionary_array::<K>(&values[0])?;
        let nvalues = arr.values().len();
        if let Some(state) = &self.state {
            if state.len() != nvalues {
                return Err(DataFusionError::Internal(
                    "Accumulator update_batch got invalid value".to_string(),
                ));
            }
        } else {
            // init state
            self.state = Some((0..nvalues).map(|_| false).collect());
        }
        for idx in arr.keys_iter().flatten() {
            self.state.as_mut().unwrap()[idx] = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let scalar = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::List(Some(scalar), _) = scalar {
                if self.state.is_none() {
                    self.state = Some((0..scalar.len()).map(|_| false).collect());
                } else if scalar.len() != self.state.as_ref().unwrap().len() {
                    return Err(DataFusionError::Internal(
                        "accumulator merged invalid state".into(),
                    ));
                }
                for (idx, val) in scalar.iter().enumerate() {
                    match val {
                        ScalarValue::Boolean(Some(b)) => {
                            if *b {
                                self.state.as_mut().unwrap()[idx] = true;
                            }
                        }
                        _ => {
                            return Err(DataFusionError::Internal(
                                "Unexpected accumulator state".into(),
                            ));
                        }
                    }
                }
            } else {
                return Err(DataFusionError::Internal(
                    "Unexpected accumulator state".into(),
                ));
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if let Some(state) = &self.state {
            let num_seen = state.iter().filter(|v| **v).count();
            Ok(ScalarValue::Int64(Some(num_seen as i64)))
        } else {
            Ok(ScalarValue::Int64(Some(0)))
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self
                .state
                .as_ref()
                .map(|state| std::mem::size_of::<bool>() * state.capacity())
                .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use crate::expressions::NoOp;

    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, DictionaryArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, StringArray, UInt16Array, UInt32Array,
        UInt64Array, UInt8Array,
    };
    use arrow::datatypes::{DataType, Int8Type};

    macro_rules! state_to_vec {
        ($LIST:expr, $DATA_TYPE:ident, $PRIM_TY:ty) => {{
            match $LIST {
                ScalarValue::List(_, field) => match field.data_type() {
                    &DataType::$DATA_TYPE => (),
                    _ => panic!("Unexpected DataType for list"),
                },
                _ => panic!("Expected a ScalarValue::List"),
            }

            match $LIST {
                ScalarValue::List(None, _) => None,
                ScalarValue::List(Some(scalar_values), _) => {
                    let vec = scalar_values
                        .iter()
                        .map(|scalar_value| match scalar_value {
                            ScalarValue::$DATA_TYPE(value) => *value,
                            _ => panic!("Unexpected ScalarValue variant"),
                        })
                        .collect::<Vec<Option<$PRIM_TY>>>();

                    Some(vec)
                }
                _ => unreachable!(),
            }
        }};
    }

    macro_rules! test_count_distinct_update_batch_numeric {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(1),
                Some(1),
                None,
                Some(3),
                Some(2),
                None,
                Some(2),
                Some(3),
                Some(1),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec =
                state_to_vec!(&states[0], $DATA_TYPE, $PRIM_TYPE).unwrap();
            state_vec.sort();

            assert_eq!(states.len(), 1);
            assert_eq!(state_vec, vec![Some(1), Some(2), Some(3)]);
            assert_eq!(result, ScalarValue::Int64(Some(3)));

            Ok(())
        }};
    }

    fn run_update_batch(arrays: &[ArrayRef]) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            arrays[0].data_type().clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    fn run_update(
        data_types: &[DataType],
        rows: &[Vec<ScalarValue>],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctCount::new(
            data_types[0].clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );

        let mut accum = agg.create_accumulator()?;

        let cols = (0..rows[0].len())
            .map(|i| {
                rows.iter()
                    .map(|inner| inner[i].clone())
                    .collect::<Vec<ScalarValue>>()
            })
            .collect::<Vec<_>>();

        let arrays: Vec<ArrayRef> = cols
            .iter()
            .map(|c| ScalarValue::iter_to_array(c.clone()))
            .collect::<Result<Vec<ArrayRef>>>()?;

        accum.update_batch(&arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    // Used trait to create associated constant for f32 and f64
    trait SubNormal: 'static {
        const SUBNORMAL: Self;
    }

    impl SubNormal for f64 {
        const SUBNORMAL: Self = 1.0e-308_f64;
    }

    impl SubNormal for f32 {
        const SUBNORMAL: Self = 1.0e-38_f32;
    }

    macro_rules! test_count_distinct_update_batch_floating_point {
        ($ARRAY_TYPE:ident, $DATA_TYPE:ident, $PRIM_TYPE:ty) => {{
            let values: Vec<Option<$PRIM_TYPE>> = vec![
                Some(<$PRIM_TYPE>::INFINITY),
                Some(<$PRIM_TYPE>::NAN),
                Some(1.0),
                Some(<$PRIM_TYPE as SubNormal>::SUBNORMAL),
                Some(1.0),
                Some(<$PRIM_TYPE>::INFINITY),
                None,
                Some(3.0),
                Some(-4.5),
                Some(2.0),
                None,
                Some(2.0),
                Some(3.0),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
                Some(1.0),
                Some(<$PRIM_TYPE>::NAN),
                Some(<$PRIM_TYPE>::NEG_INFINITY),
            ];

            let arrays = vec![Arc::new($ARRAY_TYPE::from(values)) as ArrayRef];

            let (states, result) = run_update_batch(&arrays)?;

            let mut state_vec =
                state_to_vec!(&states[0], $DATA_TYPE, $PRIM_TYPE).unwrap();
            state_vec.sort_by(|a, b| match (a, b) {
                (Some(lhs), Some(rhs)) => lhs.total_cmp(rhs),
                _ => a.partial_cmp(b).unwrap(),
            });

            let nan_idx = state_vec.len() - 1;
            assert_eq!(states.len(), 1);
            assert_eq!(
                &state_vec[..nan_idx],
                vec![
                    Some(<$PRIM_TYPE>::NEG_INFINITY),
                    Some(-4.5),
                    Some(<$PRIM_TYPE as SubNormal>::SUBNORMAL),
                    Some(1.0),
                    Some(2.0),
                    Some(3.0),
                    Some(<$PRIM_TYPE>::INFINITY)
                ]
            );
            assert!(state_vec[nan_idx].unwrap_or_default().is_nan());
            assert_eq!(result, ScalarValue::Int64(Some(8)));

            Ok(())
        }};
    }

    #[test]
    fn count_distinct_update_batch_i8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int8Array, Int8, i8)
    }

    #[test]
    fn count_distinct_update_batch_i16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int16Array, Int16, i16)
    }

    #[test]
    fn count_distinct_update_batch_i32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int32Array, Int32, i32)
    }

    #[test]
    fn count_distinct_update_batch_i64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(Int64Array, Int64, i64)
    }

    #[test]
    fn count_distinct_update_batch_u8() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt8Array, UInt8, u8)
    }

    #[test]
    fn count_distinct_update_batch_u16() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt16Array, UInt16, u16)
    }

    #[test]
    fn count_distinct_update_batch_u32() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt32Array, UInt32, u32)
    }

    #[test]
    fn count_distinct_update_batch_u64() -> Result<()> {
        test_count_distinct_update_batch_numeric!(UInt64Array, UInt64, u64)
    }

    #[test]
    fn count_distinct_update_batch_f32() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float32Array, Float32, f32)
    }

    #[test]
    fn count_distinct_update_batch_f64() -> Result<()> {
        test_count_distinct_update_batch_floating_point!(Float64Array, Float64, f64)
    }

    #[test]
    fn count_distinct_update_batch_boolean() -> Result<()> {
        let get_count = |data: BooleanArray| -> Result<(Vec<Option<bool>>, i64)> {
            let arrays = vec![Arc::new(data) as ArrayRef];
            let (states, result) = run_update_batch(&arrays)?;
            let mut state_vec = state_to_vec!(&states[0], Boolean, bool).unwrap();
            state_vec.sort();
            let count = match result {
                ScalarValue::Int64(c) => c.ok_or_else(|| {
                    DataFusionError::Internal("Found None count".to_string())
                }),
                scalar => Err(DataFusionError::Internal(format!(
                    "Found non int64 scalar value from count: {scalar}"
                ))),
            }?;
            Ok((state_vec, count))
        };

        let zero_count_values = BooleanArray::from(Vec::<bool>::new());

        let one_count_values = BooleanArray::from(vec![false, false]);
        let one_count_values_with_null =
            BooleanArray::from(vec![Some(true), Some(true), None, None]);

        let two_count_values = BooleanArray::from(vec![true, false, true, false, true]);
        let two_count_values_with_null = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            None,
            Some(true),
            Some(false),
        ]);

        assert_eq!(
            get_count(zero_count_values)?,
            (Vec::<Option<bool>>::new(), 0)
        );
        assert_eq!(get_count(one_count_values)?, (vec![Some(false)], 1));
        assert_eq!(
            get_count(one_count_values_with_null)?,
            (vec![Some(true)], 1)
        );
        assert_eq!(
            get_count(two_count_values)?,
            (vec![Some(false), Some(true)], 2)
        );
        assert_eq!(
            get_count(two_count_values_with_null)?,
            (vec![Some(false), Some(true)], 2)
        );
        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_all_nulls() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(
            vec![None, None, None, None] as Vec<Option<i32>>
        )) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(state_to_vec!(&states[0], Int32, i32), Some(vec![]));
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update_batch_empty() -> Result<()> {
        let arrays = vec![Arc::new(Int32Array::from(vec![0_i32; 0])) as ArrayRef];

        let (states, result) = run_update_batch(&arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(state_to_vec!(&states[0], Int32, i32), Some(vec![]));
        assert_eq!(result, ScalarValue::Int64(Some(0)));

        Ok(())
    }

    #[test]
    fn count_distinct_update() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32],
            &[
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(5))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(5))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(2))],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(3)));

        let (states, result) = run_update(
            &[DataType::UInt64],
            &[
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(5))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(2))],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(3)));
        Ok(())
    }

    #[test]
    fn count_distinct_update_with_nulls() -> Result<()> {
        let (states, result) = run_update(
            &[DataType::Int32],
            &[
                // None of these updates contains a None, so these are accumulated.
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(Some(-2))],
                // Each of these updates contains at least one None, so these
                // won't be accumulated.
                vec![ScalarValue::Int32(Some(-1))],
                vec![ScalarValue::Int32(None)],
                vec![ScalarValue::Int32(None)],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(2)));

        let (states, result) = run_update(
            &[DataType::UInt64],
            &[
                // None of these updates contains a None, so these are accumulated.
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(Some(2))],
                // Each of these updates contains at least one None, so these
                // won't be accumulated.
                vec![ScalarValue::UInt64(Some(1))],
                vec![ScalarValue::UInt64(None)],
                vec![ScalarValue::UInt64(None)],
            ],
        )?;
        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(2)));
        Ok(())
    }

    #[test]
    fn count_distinct_dict_update() -> Result<()> {
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        // value "b" is never used
        let keys =
            Int8Array::from_iter(vec![Some(0), Some(0), Some(0), Some(0), None, Some(2)]);
        let arrays =
            vec![
                Arc::new(DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap())
                    as ArrayRef,
            ];
        let agg = DistinctCount::new(
            arrays[0].data_type().clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );
        let mut accum = agg.create_accumulator()?;
        accum.update_batch(&arrays)?;
        // should evaluate to 2 since "b" never seen
        assert_eq!(accum.evaluate()?, ScalarValue::Int64(Some(2)));
        // now update with a new batch that does use "b"
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int8Array::from_iter(vec![Some(1), Some(1), None]);
        let arrays =
            vec![
                Arc::new(DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap())
                    as ArrayRef,
            ];
        accum.update_batch(&arrays)?;
        assert_eq!(accum.evaluate()?, ScalarValue::Int64(Some(3)));
        Ok(())
    }

    #[test]
    fn count_distinct_dict_merge() -> Result<()> {
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int8Array::from_iter(vec![Some(0), Some(0), None]);
        let arrays =
            vec![
                Arc::new(DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap())
                    as ArrayRef,
            ];
        let agg = DistinctCount::new(
            arrays[0].data_type().clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );
        // create accum with 1 value seen
        let mut accum = agg.create_accumulator()?;
        accum.update_batch(&arrays)?;
        assert_eq!(accum.evaluate()?, ScalarValue::Int64(Some(1)));
        // create accum with state that has seen "a" and "b" but not "c"
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int8Array::from_iter(vec![Some(0), Some(1), None]);
        let arrays =
            vec![
                Arc::new(DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap())
                    as ArrayRef,
            ];
        let mut accum2 = agg.create_accumulator()?;
        accum2.update_batch(&arrays)?;
        let states = accum2
            .state()?
            .into_iter()
            .map(|v| v.to_array())
            .collect::<Vec<_>>();
        // after merging the accumulator should have seen 2 vals
        accum.merge_batch(&states)?;
        assert_eq!(accum.evaluate()?, ScalarValue::Int64(Some(2)));
        Ok(())
    }

    #[test]
    fn count_distinct_dict_merge_inits_state() -> Result<()> {
        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int8Array::from_iter(vec![Some(0), Some(1), None]);
        let arrays =
            vec![
                Arc::new(DictionaryArray::<Int8Type>::try_new(&keys, &values).unwrap())
                    as ArrayRef,
            ];
        let agg = DistinctCount::new(
            arrays[0].data_type().clone(),
            Arc::new(NoOp::new()),
            String::from("__col_name__"),
        );
        // create accum to get a state from
        let mut accum = agg.create_accumulator()?;
        accum.update_batch(&arrays)?;
        let states = accum
            .state()?
            .into_iter()
            .map(|v| v.to_array())
            .collect::<Vec<_>>();
        // create accum that hasnt been initialized
        // the merge_batch should initialize its state
        let mut accum2 = agg.create_accumulator()?;
        accum2.merge_batch(&states)?;
        assert_eq!(accum2.evaluate()?, ScalarValue::Int64(Some(2)));
        Ok(())
    }
}
