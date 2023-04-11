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

use arrow::datatypes::{DataType, Field};
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

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DistinctScalarValues(Vec<ScalarValue>);

/// Expression for a COUNT(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctCount {
    /// Column name
    name: String,
    /// The DataType used to hold the state for each input
    state_data_types: Vec<DataType>,
    /// The input arguments
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl DistinctCount {
    /// Create a new COUNT(DISTINCT) aggregate function.
    pub fn new(
        input_data_types: Vec<DataType>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: String,
    ) -> Self {
        Self {
            name,
            state_data_types: input_data_types,
            exprs,
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

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctCountAccumulator {
            values: HashSet::default(),
            state_data_types: self.state_data_types.clone(),
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(self
            .state_data_types
            .iter()
            .map(|state_data_type| {
                Field::new(
                    format_state_name(&self.name, "count distinct"),
                    DataType::List(Box::new(Field::new(
                        "item",
                        state_data_type.clone(),
                        true,
                    ))),
                    false,
                )
            })
            .collect::<Vec<_>>())
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<DistinctScalarValues, RandomState>,
    state_data_types: Vec<DataType>,
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
                .map(|vals| {
                    (ScalarValue::size_of_vec(&vals.0) - std::mem::size_of_val(&vals.0))
                        * self.values.capacity()
                })
                .unwrap_or(0)
            + (std::mem::size_of::<DataType>() * self.state_data_types.capacity())
    }

    // calculates the size as accurate as possible, call to this method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<DistinctScalarValues>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| {
                    ScalarValue::size_of_vec(&vals.0) - std::mem::size_of_val(&vals.0)
                })
                .sum::<usize>()
            + (std::mem::size_of::<DataType>() * self.state_data_types.capacity())
            + self
                .state_data_types
                .iter()
                .map(|dt| dt.size() - std::mem::size_of_val(dt))
                .sum::<usize>()
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        // If a row has a NULL, it is not included in the final count.
        if !values.iter().any(|v| v.is_null()) {
            self.values.insert(DistinctScalarValues(values.to_vec()));
        }
        Ok(())
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut cols_out = self
            .state_data_types
            .iter()
            .map(|state_data_type| {
                ScalarValue::new_list(Some(Vec::new()), state_data_type.clone())
            })
            .collect::<Vec<_>>();

        let mut cols_vec = cols_out
            .iter_mut()
            .map(|c| match c {
                ScalarValue::List(Some(ref mut v), _) => Ok(v),
                t => Err(DataFusionError::Internal(format!(
                    "cols_out should only consist of ScalarValue::List. {t:?} is found"
                ))),
            })
            .collect::<Result<Vec<_>>>()?;

        self.values.iter().for_each(|distinct_values| {
            distinct_values.0.iter().enumerate().for_each(
                |(col_index, distinct_value)| {
                    cols_vec[col_index].push(distinct_value.clone());
                },
            )
        });
        Ok(cols_out.into_iter().collect())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let vals = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.update(&vals)
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let vals = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;

            let col_values = vals
                .iter()
                .map(|state| match state {
                    ScalarValue::List(Some(values), _) => Ok(values),
                    _ => Err(DataFusionError::Internal(format!(
                        "Unexpected accumulator state {state:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;

            (0..col_values[0].len()).try_for_each(|row_index| {
                let row_values = col_values
                    .iter()
                    .map(|col| col[row_index].clone())
                    .collect::<Vec<_>>();
                self.update(&row_values)
            })
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        let is_fixed_length = self.state_data_types.iter().all(|t| match t {
            DataType::Boolean | DataType::Null => true,
            d if d.is_primitive() => true,
            _ => false,
        });
        if is_fixed_length {
            self.fixed_size()
        } else {
            self.full_size()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow::datatypes::DataType;

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
            arrays
                .iter()
                .map(|a| a.data_type().clone())
                .collect::<Vec<_>>(),
            vec![],
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
        let agg =
            DistinctCount::new(data_types.to_vec(), vec![], String::from("__col_name__"));

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

            dbg!(&state_vec);
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
}
