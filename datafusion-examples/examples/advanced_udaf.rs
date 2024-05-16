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

use arrow_schema::{Field, Schema};
use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility};
use datafusion_physical_expr::NullState;
use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        ArrayRef, AsArray, Float32Array, PrimitiveArray, PrimitiveBuilder, UInt32Array,
    },
    datatypes::{ArrowNativeTypeOp, ArrowPrimitiveType, Float64Type, UInt32Type},
    record_batch::RecordBatch,
};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{cast::as_float64_array, ScalarValue};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    Accumulator, AggregateUDF, AggregateUDFImpl, GroupsAccumulator, Signature,
};

/// This example shows how to use the full AggregateUDFImpl API to implement a user
/// defined aggregate function. As in the `simple_udaf.rs` example, this struct implements
/// a function `accumulator` that returns the `Accumulator` instance.
///
/// To do so, we must implement the `AggregateUDFImpl` trait.
#[derive(Debug, Clone)]
struct GeoMeanUdaf {
    signature: Signature,
}

impl GeoMeanUdaf {
    /// Create a new instance of the GeoMeanUdaf struct
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                // this function will always take one arguments of type f64
                vec![DataType::Float64],
                // this function is deterministic and will always return the same
                // result for the same input
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for GeoMeanUdaf {
    /// We implement as_any so that we can downcast the AggregateUDFImpl trait object
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Return the name of this function
    fn name(&self) -> &str {
        "geo_mean"
    }

    /// Return the "signature" of this function -- namely that types of arguments it will take
    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What is the type of value that will be returned by this function.
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    /// This is the accumulator factory; DataFusion uses it to create new accumulators.
    ///
    /// This is the accumulator factory for row wise accumulation; Even when `GroupsAccumulator`
    /// is supported, DataFusion will use this row oriented
    /// accumulator when the aggregate function is used as a window function
    /// or when there are only aggregates (no GROUP BY columns) in the plan.
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(GeometricMean::new()))
    }

    /// This is the description of the state. accumulator's state() must match the types here.
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow_schema::Field>> {
        Ok(vec![
            Field::new("prod", args.return_type.clone(), true),
            Field::new("n", DataType::UInt32, true),
        ])
    }

    /// Tell DataFusion that this aggregate supports the more performant `GroupsAccumulator`
    /// which is used for cases when there are grouping columns in the query
    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(GeometricMeanGroupsAccumulator::new()))
    }
}

/// A UDAF has state across multiple rows, and thus we require a `struct` with that state.
#[derive(Debug)]
struct GeometricMean {
    n: u32,
    prod: f64,
}

impl GeometricMean {
    // how the struct is initialized
    pub fn new() -> Self {
        GeometricMean { n: 0, prod: 1.0 }
    }
}

// UDAFs are built using the trait `Accumulator`, that offers DataFusion the necessary functions
// to use them.
impl Accumulator for GeometricMean {
    // This function serializes our state to `ScalarValue`, which DataFusion uses
    // to pass this state between execution stages.
    // Note that this can be arbitrary data.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.prod),
            ScalarValue::from(self.n),
        ])
    }

    // DataFusion expects this function to return the final value of this aggregator.
    // in this case, this is the formula of the geometric mean
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let value = self.prod.powf(1.0 / self.n as f64);
        Ok(ScalarValue::from(value))
    }

    // DataFusion calls this function to update the accumulator's state for a batch
    // of inputs rows. In this case the product is updated with values from the first column
    // and the count is updated based on the row count
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = &values[0];
        (0..arr.len()).try_for_each(|index| {
            let v = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::Float64(Some(value)) = v {
                self.prod *= value;
                self.n += 1;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    // Merge the output of `Self::state()` from other instances of this accumulator
    // into this accumulator's state
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            if let (ScalarValue::Float64(Some(prod)), ScalarValue::UInt32(Some(n))) =
                (&v[0], &v[1])
            {
                self.prod *= prod;
                self.n += n;
            } else {
                unreachable!("")
            }
            Ok(())
        })
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

// create local session context with an in-memory table
fn create_context() -> Result<SessionContext> {
    use datafusion::datasource::MemTable;
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float32, false),
    ]));

    // define data in two partitions
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0])),
            Arc::new(Float32Array::from(vec![2.0, 2.0, 2.0])),
        ],
    )?;
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![64.0])),
            Arc::new(Float32Array::from(vec![2.0])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    let provider = MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?;
    ctx.register_table("t", Arc::new(provider))?;
    Ok(ctx)
}

// Define a `GroupsAccumulator` for GeometricMean
/// which handles accumulator state for multiple groups at once.
/// This API is significantly more complicated than `Accumulator`, which manages
/// the state for a single group, but for queries with a large number of groups
/// can be significantly faster. See the `GroupsAccumulator` documentation for
/// more information.
struct GeometricMeanGroupsAccumulator {
    /// The type of the internal sum
    prod_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u32 to make UInt32Array)
    counts: Vec<u32>,

    /// product per group, stored as the native type (not `ScalarValue`)
    prods: Vec<f64>,

    /// Track nulls in the input / filters
    null_state: NullState,
}

impl GeometricMeanGroupsAccumulator {
    fn new() -> Self {
        Self {
            prod_data_type: DataType::Float64,
            return_data_type: DataType::Float64,
            counts: vec![],
            prods: vec![],
            null_state: NullState::new(),
        }
    }
}

impl GroupsAccumulator for GeometricMeanGroupsAccumulator {
    /// Updates the accumulator state given input. DataFusion provides `group_indices`,
    /// the groups that each row in `values` belongs to as well as an optional filter of which rows passed.
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
        _grouping_set: &[bool],
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<Float64Type>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.prods.resize(total_num_groups, 1.0);
        // Use the `NullState` structure to generate specialized code for null / non null input elements
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let prod = &mut self.prods[group_index];
                *prod = prod.mul_wrapping(new_value);

                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    /// Merge the results from previous invocations of `evaluate` into this accumulator's state
    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow::array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_prods = values[0].as_primitive::<Float64Type>();
        let partial_counts = values[1].as_primitive::<UInt32Type>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                self.counts[group_index] += partial_count;
            },
        );

        // update prods
        self.prods.resize(total_num_groups, 1.0);
        self.null_state.accumulate(
            group_indices,
            partial_prods,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <Float64Type as ArrowPrimitiveType>::Native| {
                let prod = &mut self.prods[group_index];
                *prod = prod.mul_wrapping(new_value);
            },
        );

        Ok(())
    }

    /// Generate output, as specififed by `emit_to` and update the intermediate state
    fn evaluate(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let prods = emit_to.take_needed(&mut self.prods);
        let nulls = self.null_state.build(emit_to);

        assert_eq!(nulls.len(), prods.len());
        assert_eq!(counts.len(), prods.len());

        // don't evaluate geometric mean with null inputs to avoid errors on null values

        let array: PrimitiveArray<Float64Type> = if nulls.null_count() > 0 {
            let mut builder = PrimitiveBuilder::<Float64Type>::with_capacity(nulls.len());
            let iter = prods.into_iter().zip(counts).zip(nulls.iter());

            for ((prod, count), is_valid) in iter {
                if is_valid {
                    builder.append_value(prod.powf(1.0 / count as f64))
                } else {
                    builder.append_null();
                }
            }
            builder.finish()
        } else {
            let geo_mean: Vec<<Float64Type as ArrowPrimitiveType>::Native> = prods
                .into_iter()
                .zip(counts)
                .map(|(prod, count)| prod.powf(1.0 / count as f64))
                .collect::<Vec<_>>();
            PrimitiveArray::new(geo_mean.into(), Some(nulls)) // no copy
                .with_data_type(self.return_data_type.clone())
        };

        Ok(Arc::new(array))
    }

    // return arrays for counts and prods
    fn state(&mut self, emit_to: datafusion_expr::EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to);
        let nulls = Some(nulls);

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = UInt32Array::new(counts.into(), nulls.clone()); // zero copy

        let prods = emit_to.take_needed(&mut self.prods);
        let prods = PrimitiveArray::<Float64Type>::new(prods.into(), nulls) // zero copy
            .with_data_type(self.prod_data_type.clone());

        Ok(vec![
            Arc::new(prods) as ArrayRef,
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u32>()
            + self.prods.capacity() * std::mem::size_of::<Float64Type>()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = create_context()?;

    // create the AggregateUDF
    let geometric_mean = AggregateUDF::from(GeoMeanUdaf::new());
    ctx.register_udaf(geometric_mean.clone());

    let sql_df = ctx.sql("SELECT geo_mean(a) FROM t group by b").await?;
    sql_df.show().await?;

    // get a DataFrame from the context
    // this table has 1 column `a` f32 with values {2,4,8,64}, whose geometric mean is 8.0.
    let df = ctx.table("t").await?;

    // perform the aggregation
    let df = df.aggregate(vec![], vec![geometric_mean.call(vec![col("a")])])?;

    // note that "a" is f32, not f64. DataFusion coerces it to match the UDAF's signature.

    // execute the query
    let results = df.collect().await?;

    // downcast the array to the expected type
    let result = as_float64_array(results[0].column(0))?;

    // verify that the calculation is correct
    assert!((result.value(0) - 8.0).abs() < f64::EPSILON);
    println!("The geometric mean of [2,4,8,64] is {}", result.value(0));

    Ok(())
}
