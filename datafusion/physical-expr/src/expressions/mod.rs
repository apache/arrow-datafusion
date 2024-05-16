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

//! Defines physical expressions that can evaluated at runtime during query execution

#[macro_use]
mod binary;
mod case;
mod cast;
mod column;
mod datum;
mod in_list;
mod is_not_null;
mod is_null;
mod like;
mod literal;
mod negative;
mod no_op;
mod not;
mod try_cast;

/// Module with some convenient methods used in expression building
pub mod helpers {
    pub use crate::aggregate::min_max::{max, min};
}

pub use crate::aggregate::approx_distinct::ApproxDistinct;
pub use crate::aggregate::approx_median::ApproxMedian;
pub use crate::aggregate::approx_percentile_cont::ApproxPercentileCont;
pub use crate::aggregate::approx_percentile_cont_with_weight::ApproxPercentileContWithWeight;
pub use crate::aggregate::array_agg::ArrayAgg;
pub use crate::aggregate::array_agg_distinct::DistinctArrayAgg;
pub use crate::aggregate::array_agg_ordered::OrderSensitiveArrayAgg;
pub use crate::aggregate::average::{Avg, AvgAccumulator};
pub use crate::aggregate::bit_and_or_xor::{BitAnd, BitOr, BitXor, DistinctBitXor};
pub use crate::aggregate::bool_and_or::{BoolAnd, BoolOr};
pub use crate::aggregate::build_in::create_aggregate_expr;
pub use crate::aggregate::correlation::Correlation;
pub use crate::aggregate::count::Count;
pub use crate::aggregate::count_distinct::DistinctCount;
pub use crate::aggregate::grouping::Grouping;
pub use crate::aggregate::median::Median;
pub use crate::aggregate::min_max::{Max, Min};
pub use crate::aggregate::min_max::{MaxAccumulator, MinAccumulator};
pub use crate::aggregate::nth_value::NthValueAgg;
pub use crate::aggregate::regr::{Regr, RegrType};
pub use crate::aggregate::stats::StatsType;
pub use crate::aggregate::stddev::{Stddev, StddevPop};
pub use crate::aggregate::string_agg::StringAgg;
pub use crate::aggregate::sum::Sum;
pub use crate::aggregate::sum_distinct::DistinctSum;
pub use crate::aggregate::variance::{Variance, VariancePop};
pub use crate::window::cume_dist::cume_dist;
pub use crate::window::cume_dist::CumeDist;
pub use crate::window::lead_lag::WindowShift;
pub use crate::window::lead_lag::{lag, lead};
pub use crate::window::nth_value::NthValue;
pub use crate::window::ntile::Ntile;
pub use crate::window::rank::{dense_rank, percent_rank, rank};
pub use crate::window::rank::{Rank, RankType};
pub use crate::window::row_number::RowNumber;
pub use crate::PhysicalSortExpr;
pub use datafusion_functions_aggregate::first_last::{
    FirstValuePhysicalExpr as FirstValue, LastValuePhysicalExpr as LastValue,
};

pub use binary::{binary, BinaryExpr};
pub use case::{case, CaseExpr};
pub use cast::{cast, cast_with_options, CastExpr};
pub use column::UnKnownColumn;
pub use datafusion_expr::utils::format_state_name;
pub use datafusion_physical_expr_common::expressions::column::{col, Column};
pub use in_list::{in_list, InListExpr};
pub use is_not_null::{is_not_null, IsNotNullExpr};
pub use is_null::{is_null, IsNullExpr};
pub use like::{like, LikeExpr};
pub use literal::{lit, Literal};
pub use negative::{negative, NegativeExpr};
pub use no_op::NoOp;
pub use not::{not, NotExpr};
pub use try_cast::{try_cast, TryCastExpr};

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use crate::AggregateExpr;
    use arrow::record_batch::RecordBatch;
    use arrow_array::ArrayRef;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::EmitTo;

    /// macro to perform an aggregation using [`datafusion_expr::Accumulator`] and verify the
    /// result.
    #[macro_export]
    macro_rules! generic_test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr) => {
            generic_test_op!($ARRAY, $DATATYPE, $OP, $EXPECTED, $EXPECTED.data_type())
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(()) as Result<(), ::datafusion_common::DataFusionError>
        }};
    }

    /// Same as [`generic_test_op`] but with support for providing a 4th argument, usually
    /// a boolean to indicate if using the distinct version of the op.
    #[macro_export]
    macro_rules! generic_test_distinct_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $DISTINCT:expr, $EXPECTED:expr) => {
            generic_test_distinct_op!(
                $ARRAY,
                $DATATYPE,
                $OP,
                $DISTINCT,
                $EXPECTED,
                $EXPECTED.data_type()
            )
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $DISTINCT:expr, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
                $DISTINCT,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(()) as Result<(), ::datafusion_common::DataFusionError>
        }};
    }

    /// macro to perform an aggregation using [`crate::GroupsAccumulator`] and verify the result.
    ///
    /// The difference between this and the above `generic_test_op` is that the former checks
    /// the old slow-path [`datafusion_expr::Accumulator`] implementation, while this checks
    /// the new [`crate::GroupsAccumulator`] implementation.
    #[macro_export]
    macro_rules! generic_test_op_new {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr) => {
            generic_test_op_new!(
                $ARRAY,
                $DATATYPE,
                $OP,
                $EXPECTED,
                $EXPECTED.data_type().clone()
            )
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
            ));
            let actual = aggregate_new(&batch, agg)?;
            assert_eq!($EXPECTED, &actual);

            Ok(()) as Result<(), ::datafusion_common::DataFusionError>
        }};
    }

    /// macro to perform an aggregation with two inputs and verify the result.
    #[macro_export]
    macro_rules! generic_test_op2 {
        ($ARRAY1:expr, $ARRAY2:expr, $DATATYPE1:expr, $DATATYPE2:expr, $OP:ident, $EXPECTED:expr) => {
            generic_test_op2!(
                $ARRAY1,
                $ARRAY2,
                $DATATYPE1,
                $DATATYPE2,
                $OP,
                $EXPECTED,
                $EXPECTED.data_type()
            )
        };
        ($ARRAY1:expr, $ARRAY2:expr, $DATATYPE1:expr, $DATATYPE2:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![
                Field::new("a", $DATATYPE1, true),
                Field::new("b", $DATATYPE2, true),
            ]);
            let batch =
                RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY1, $ARRAY2])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                col("b", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(())
        }};
    }

    pub fn aggregate(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ScalarValue> {
        let mut accum = agg.create_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }

    pub fn aggregate_new(
        batch: &RecordBatch,
        agg: Arc<dyn AggregateExpr>,
    ) -> Result<ArrayRef> {
        let mut accum = agg.create_groups_accumulator()?;
        let expr = agg.expressions();
        let values = expr
            .iter()
            .map(|e| {
                e.evaluate(batch)
                    .and_then(|v| v.into_array(batch.num_rows()))
            })
            .collect::<Result<Vec<_>>>()?;
        let indices = vec![0; batch.num_rows()];
        accum.update_batch(&values, &indices, None, 1, &[])?;
        accum.evaluate(EmitTo::All)
    }
}
