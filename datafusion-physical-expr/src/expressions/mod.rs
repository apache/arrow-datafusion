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

mod approx_distinct;
mod approx_percentile_cont;
mod array_agg;
mod average;
#[cfg(feature = "roaring_bitmap")]
mod bitmap_distinct;
#[macro_use]
mod binary;
mod case;
mod cast;
mod column;
mod count;
mod cume_dist;
mod get_indexed_field;
mod in_list;
mod is_not_null;
mod is_null;
mod lead_lag;
mod literal;
#[macro_use]
mod min_max;
mod approx_median;
mod correlation;
mod covariance;
mod distinct_expressions;
mod negative;
mod not;
mod nth_value;
mod nullif;
mod rank;
mod row_number;
mod stats;
mod stddev;
mod sum;
mod try_cast;
mod variance;

/// Module with some convenient methods used in expression building
pub mod helpers {
    pub use super::min_max::{max, min};
}

pub use approx_distinct::ApproxDistinct;
pub use approx_median::ApproxMedian;
pub use approx_percentile_cont::{
    is_approx_percentile_cont_supported_arg_type, ApproxPercentileCont,
};
pub use array_agg::ArrayAgg;
pub use average::is_avg_support_arg_type;
pub use average::{avg_return_type, Avg, AvgAccumulator};
pub use binary::{binary, binary_operator_data_type, BinaryExpr};
#[cfg(feature = "roaring_bitmap")]
pub use bitmap_distinct::{is_bitmap_count_distinct_supported_arg_type, BitMapDistinct};
pub use case::{case, CaseExpr};
pub use cast::{
    cast, cast_column, cast_with_options, CastExpr, DEFAULT_DATAFUSION_CAST_OPTIONS,
};
pub use column::{col, Column};
pub use correlation::{
    correlation_return_type, is_correlation_support_arg_type, Correlation,
};
pub use count::Count;
pub use covariance::{
    covariance_return_type, is_covariance_support_arg_type, Covariance, CovariancePop,
};
pub use cume_dist::cume_dist;

pub use distinct_expressions::{DistinctArrayAgg, DistinctCount};
pub use get_indexed_field::GetIndexedFieldExpr;
pub use in_list::{in_list, InListExpr};
pub use is_not_null::{is_not_null, IsNotNullExpr};
pub use is_null::{is_null, IsNullExpr};
pub use lead_lag::{lag, lead};
pub use literal::{lit, Literal};
pub use min_max::{Max, Min};
pub use min_max::{MaxAccumulator, MinAccumulator};
pub use negative::{negative, NegativeExpr};
pub use not::{not, NotExpr};
pub use nth_value::NthValue;
pub use nullif::{nullif_func, SUPPORTED_NULLIF_TYPES};
pub use rank::{dense_rank, percent_rank, rank};
pub use row_number::RowNumber;
pub use stats::StatsType;
pub use stddev::{is_stddev_support_arg_type, stddev_return_type, Stddev, StddevPop};
pub use sum::is_sum_support_arg_type;
pub use sum::{sum_return_type, Sum};
pub use try_cast::{try_cast, TryCastExpr};
pub use variance::{
    is_variance_support_arg_type, variance_return_type, Variance, VariancePop,
};

/// returns the name of the state
pub fn format_state_name(name: &str, state_name: &str) -> String {
    format!("{}[{}]", name, state_name)
}
pub use crate::PhysicalSortExpr;

#[cfg(test)]
mod tests {
    use crate::AggregateExpr;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    /// macro to perform an aggregation and verify the result.
    #[macro_export]
    macro_rules! generic_test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, false)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(())
        }};
    }

    /// macro to perform an aggregation with two inputs and verify the result.
    #[macro_export]
    macro_rules! generic_test_op2 {
        ($ARRAY1:expr, $ARRAY2:expr, $DATATYPE1:expr, $DATATYPE2:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![
                Field::new("a", $DATATYPE1, false),
                Field::new("b", $DATATYPE2, false),
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
            .map(|e| e.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        accum.update_batch(&values)?;
        accum.evaluate()
    }
}
