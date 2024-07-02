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

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Schema};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::ProjectionMask;
use parquet::file::metadata::ParquetMetaData;

use crate::datasource::schema_adapter::SchemaMapper;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{arrow_err, DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::utils::reassign_predicate_columns;
use datafusion_physical_expr::{split_conjunction, PhysicalExpr};

use crate::physical_plan::metrics;

use super::ParquetFileMetrics;

/// This module contains utilities for enabling the pushdown of DataFusion filter predicates (which
/// can be any DataFusion `Expr` that evaluates to a `BooleanArray`) to the parquet decoder level in `arrow-rs`.
/// DataFusion will use a `ParquetRecordBatchStream` to read data from parquet into arrow `RecordBatch`es.
/// When constructing the  `ParquetRecordBatchStream` you can provide a `RowFilter` which is itself just a vector
/// of `Box<dyn ArrowPredicate>`. During decoding, the predicates are evaluated to generate a mask which is used
/// to avoid decoding rows in projected columns which are not selected which can significantly reduce the amount
/// of compute required for decoding.
///
/// Since the predicates are applied serially in the order defined in the `RowFilter`, the optimal ordering
/// will depend on the exact filters. The best filters to execute first have two properties:
///     1. The are relatively inexpensive to evaluate (e.g. they read column chunks which are relatively small)
///     2. They filter a lot of rows, reducing the amount of decoding required for subsequent filters and projected columns
///
/// Given the metadata exposed by parquet, the selectivity of filters is not easy to estimate so the heuristics we use here primarily
/// focus on the evaluation cost.
///
/// The basic algorithm for constructing the `RowFilter` is as follows
///     1. Recursively break conjunctions into separate predicates. An expression like `a = 1 AND (b = 2 AND c = 3)` would be
///        separated into the expressions `a = 1`, `b = 2`, and `c = 3`.
///     2. Determine whether each predicate is suitable as an `ArrowPredicate`. As long as the predicate does not reference any projected columns
///        or columns with non-primitive types, then it is considered suitable.
///     3. Determine, for each predicate, the total compressed size of all columns required to evaluate the predicate.
///     4. Determine, for each predicate, whether all columns required to evaluate the expression are sorted.
///     5. Re-order the predicate by total size (from step 3).
///     6. Partition the predicates according to whether they are sorted (from step 4)
///     7. "Compile" each predicate `Expr` to a `DatafusionArrowPredicate`.
///     8. Build the `RowFilter` with the sorted predicates followed by the unsorted predicates. Within each partition
///        the predicates will still be sorted by size.

/// A predicate which can be passed to `ParquetRecordBatchStream` to perform row-level
/// filtering during parquet decoding.
#[derive(Debug)]
pub(crate) struct DatafusionArrowPredicate {
    physical_expr: Arc<dyn PhysicalExpr>,
    projection_mask: ProjectionMask,
    projection: Vec<usize>,
    /// how many rows were filtered out by this predicate
    rows_filtered: metrics::Count,
    /// how long was spent evaluating this predicate
    time: metrics::Time,
    /// used to perform type coercion while filtering rows
    schema_mapping: Arc<dyn SchemaMapper>,
}

impl DatafusionArrowPredicate {
    pub fn try_new(
        candidate: FilterCandidate,
        schema: &Schema,
        metadata: &ParquetMetaData,
        rows_filtered: metrics::Count,
        time: metrics::Time,
        schema_mapping: Arc<dyn SchemaMapper>,
    ) -> Result<Self> {
        let schema = Arc::new(schema.project(&candidate.projection)?);
        let physical_expr = reassign_predicate_columns(candidate.expr, &schema, true)?;

        // ArrowPredicate::evaluate is passed columns in the order they appear in the file
        // If the predicate has multiple columns, we therefore must project the columns based
        // on the order they appear in the file
        let projection = match candidate.projection.len() {
            0 | 1 => vec![],
            _ => remap_projection(&candidate.projection),
        };

        Ok(Self {
            physical_expr,
            projection,
            projection_mask: ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                candidate.projection,
            ),
            rows_filtered,
            time,
            schema_mapping,
        })
    }
}

impl ArrowPredicate for DatafusionArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> ArrowResult<BooleanArray> {
        let batch = match self.projection.is_empty() {
            true => batch,
            false => batch.project(&self.projection)?,
        };

        let batch = self.schema_mapping.map_partial_batch(batch)?;

        // scoped timer updates on drop
        let mut timer = self.time.timer();
        match self
            .physical_expr
            .evaluate(&batch)
            .and_then(|v| v.into_array(batch.num_rows()))
        {
            Ok(array) => {
                let bool_arr = as_boolean_array(&array)?.clone();
                let num_filtered = bool_arr.len() - bool_arr.true_count();
                self.rows_filtered.add(num_filtered);
                timer.stop();
                Ok(bool_arr)
            }
            Err(e) => Err(ArrowError::ComputeError(format!(
                "Error evaluating filter predicate: {e:?}"
            ))),
        }
    }
}

/// A candidate expression for creating a `RowFilter` contains the
/// expression as well as data to estimate the cost of evaluating
/// the resulting expression.
pub(crate) struct FilterCandidate {
    expr: Arc<dyn PhysicalExpr>,
    required_bytes: usize,
    can_use_index: bool,
    projection: Vec<usize>,
}

/// Helper to build a `FilterCandidate`. This will do several things
/// 1. Determine the columns required to evaluate the expression
/// 2. Calculate data required to estimate the cost of evaluating the filter
/// 3. Rewrite column expressions in the predicate which reference columns not in the particular file schema.
///    This is relevant in the case where we have determined the table schema by merging all individual file schemas
///    and any given file may or may not contain all columns in the merged schema. If a particular column is not present
///    we replace the column expression with a literal expression that produces a null value.
struct FilterCandidateBuilder<'a> {
    expr: Arc<dyn PhysicalExpr>,
    file_schema: &'a Schema,
    table_schema: &'a Schema,
    required_column_indices: BTreeSet<usize>,
    non_primitive_columns: bool,
    projected_columns: bool,
}

impl<'a> FilterCandidateBuilder<'a> {
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        file_schema: &'a Schema,
        table_schema: &'a Schema,
    ) -> Self {
        Self {
            expr,
            file_schema,
            table_schema,
            required_column_indices: BTreeSet::default(),
            non_primitive_columns: false,
            projected_columns: false,
        }
    }

    pub fn build(
        mut self,
        metadata: &ParquetMetaData,
    ) -> Result<Option<FilterCandidate>> {
        let expr = Arc::clone(&self.expr).rewrite(&mut self).data()?;

        if self.non_primitive_columns || self.projected_columns {
            Ok(None)
        } else {
            let required_bytes =
                size_of_columns(&self.required_column_indices, metadata)?;
            let can_use_index = columns_sorted(&self.required_column_indices, metadata)?;

            Ok(Some(FilterCandidate {
                expr,
                required_bytes,
                can_use_index,
                projection: self.required_column_indices.into_iter().collect(),
            }))
        }
    }
}

impl<'a> TreeNodeRewriter for FilterCandidateBuilder<'a> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(
        &mut self,
        node: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            if let Ok(idx) = self.file_schema.index_of(column.name()) {
                self.required_column_indices.insert(idx);

                if DataType::is_nested(self.file_schema.field(idx).data_type()) {
                    self.non_primitive_columns = true;
                    return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
                }
            } else if self.table_schema.index_of(column.name()).is_err() {
                // If the column does not exist in the (un-projected) table schema then
                // it must be a projected column.
                self.projected_columns = true;
                return Ok(Transformed::new(node, false, TreeNodeRecursion::Jump));
            }
        }

        Ok(Transformed::no(node))
    }

    fn f_up(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            if self.file_schema.field_with_name(column.name()).is_err() {
                // the column expr must be in the table schema
                return match self.table_schema.field_with_name(column.name()) {
                    Ok(field) => {
                        // return the null value corresponding to the data type
                        let null_value = ScalarValue::try_from(field.data_type())?;
                        Ok(Transformed::yes(Arc::new(Literal::new(null_value))))
                    }
                    Err(e) => {
                        // If the column is not in the table schema, should throw the error
                        arrow_err!(e)
                    }
                };
            }
        }

        Ok(Transformed::no(expr))
    }
}

/// Computes the projection required to go from the file's schema order to the projected
/// order expected by this filter
///
/// Effectively this computes the rank of each element in `src`
fn remap_projection(src: &[usize]) -> Vec<usize> {
    let len = src.len();

    // Compute the column mapping from projected order to file order
    // i.e. the indices required to sort projected schema into the file schema
    //
    // e.g. projection: [5, 9, 0] -> [2, 0, 1]
    let mut sorted_indexes: Vec<_> = (0..len).collect();
    sorted_indexes.sort_unstable_by_key(|x| src[*x]);

    // Compute the mapping from schema order to projected order
    // i.e. the indices required to sort file schema into the projected schema
    //
    // Above we computed the order of the projected schema according to the file
    // schema, and so we can use this as the comparator
    //
    // e.g. sorted_indexes [2, 0, 1] -> [1, 2, 0]
    let mut projection: Vec<_> = (0..len).collect();
    projection.sort_unstable_by_key(|x| sorted_indexes[*x]);
    projection
}

/// Calculate the total compressed size of all `Column's required for
/// predicate `Expr`. This should represent the total amount of file IO
/// required to evaluate the predicate.
fn size_of_columns(
    columns: &BTreeSet<usize>,
    metadata: &ParquetMetaData,
) -> Result<usize> {
    let mut total_size = 0;
    let row_groups = metadata.row_groups();
    for idx in columns {
        for rg in row_groups.iter() {
            total_size += rg.column(*idx).compressed_size() as usize;
        }
    }

    Ok(total_size)
}

/// For a given set of `Column`s required for predicate `Expr` determine whether all
/// columns are sorted. Sorted columns may be queried more efficiently in the presence of
/// a PageIndex.
fn columns_sorted(
    _columns: &BTreeSet<usize>,
    _metadata: &ParquetMetaData,
) -> Result<bool> {
    // TODO How do we know this?
    Ok(false)
}

/// Build a [`RowFilter`] from the given predicate `Expr`
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &Schema,
    table_schema: &Schema,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
    schema_mapping: Arc<dyn SchemaMapper>,
) -> Result<Option<RowFilter>> {
    let rows_filtered = &file_metrics.pushdown_rows_filtered;
    let time = &file_metrics.pushdown_eval_time;

    let predicates = split_conjunction(expr);

    let mut candidates: Vec<FilterCandidate> = predicates
        .into_iter()
        .flat_map(|expr| {
            if let Ok(candidate) =
                FilterCandidateBuilder::new(Arc::clone(expr), file_schema, table_schema)
                    .build(metadata)
            {
                candidate
            } else {
                None
            }
        })
        .collect();

    if candidates.is_empty() {
        Ok(None)
    } else if reorder_predicates {
        candidates.sort_by_key(|c| c.required_bytes);

        let (indexed_candidates, other_candidates): (Vec<_>, Vec<_>) =
            candidates.into_iter().partition(|c| c.can_use_index);

        let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];

        for candidate in indexed_candidates {
            let filter = DatafusionArrowPredicate::try_new(
                candidate,
                file_schema,
                metadata,
                rows_filtered.clone(),
                time.clone(),
                Arc::clone(&schema_mapping),
            )?;

            filters.push(Box::new(filter));
        }

        for candidate in other_candidates {
            let filter = DatafusionArrowPredicate::try_new(
                candidate,
                file_schema,
                metadata,
                rows_filtered.clone(),
                time.clone(),
                Arc::clone(&schema_mapping),
            )?;

            filters.push(Box::new(filter));
        }

        Ok(Some(RowFilter::new(filters)))
    } else {
        let mut filters: Vec<Box<dyn ArrowPredicate>> = vec![];
        for candidate in candidates {
            let filter = DatafusionArrowPredicate::try_new(
                candidate,
                file_schema,
                metadata,
                rows_filtered.clone(),
                time.clone(),
                Arc::clone(&schema_mapping),
            )?;

            filters.push(Box::new(filter));
        }

        Ok(Some(RowFilter::new(filters)))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::datasource::schema_adapter::DefaultSchemaAdapterFactory;
    use crate::datasource::schema_adapter::SchemaAdapterFactory;

    use arrow::datatypes::Field;
    use arrow_schema::TimeUnit::Nanosecond;
    use datafusion_expr::{cast, col, lit, Expr};
    use datafusion_physical_expr::planner::logical2physical;
    use datafusion_physical_plan::metrics::{Count, Time};

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use rand::prelude::*;

    // We should ignore predicate that read non-primitive columns
    #[test]
    fn test_filter_candidate_builder_ignore_complex_types() {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/list_columns.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let expr = col("int64_list").is_not_null();
        let expr = logical2physical(&expr, &table_schema);

        let candidate = FilterCandidateBuilder::new(expr, &table_schema, &table_schema)
            .build(metadata)
            .expect("building candidate");

        assert!(candidate.is_none());
    }

    // If a column exists in the table schema but not the file schema it should be rewritten to a null expression
    #[test]
    fn test_filter_candidate_builder_rewrite_missing_column() {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let reader = SerializedFileReader::new(file).expect("creating reader");

        let metadata = reader.metadata();

        let table_schema =
            parquet_to_arrow_schema(metadata.file_metadata().schema_descr(), None)
                .expect("parsing schema");

        let file_schema = Schema::new(vec![
            Field::new("bigint_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float32, true),
        ]);

        // The parquet file with `file_schema` just has `bigint_col` and `float_col` column, and don't have the `int_col`
        let expr = col("bigint_col").eq(cast(col("int_col"), DataType::Int64));
        let expr = logical2physical(&expr, &table_schema);
        let expected_candidate_expr =
            col("bigint_col").eq(cast(lit(ScalarValue::Int32(None)), DataType::Int64));
        let expected_candidate_expr =
            logical2physical(&expected_candidate_expr, &table_schema);

        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(metadata)
            .expect("building candidate");

        assert!(candidate.is_some());

        assert_eq!(
            candidate.unwrap().expr.to_string(),
            expected_candidate_expr.to_string()
        );
    }

    #[test]
    fn test_filter_type_coercion() {
        let testdata = crate::test_util::parquet_test_data();
        let file = std::fs::File::open(format!("{testdata}/alltypes_plain.parquet"))
            .expect("opening file");

        let parquet_reader_builder =
            ParquetRecordBatchReaderBuilder::try_new(file).expect("creating reader");
        let metadata = parquet_reader_builder.metadata().clone();
        let file_schema = parquet_reader_builder.schema().clone();

        // This is the schema we would like to coerce to,
        // which is different from the physical schema of the file.
        let table_schema = Schema::new(vec![Field::new(
            "timestamp_col",
            DataType::Timestamp(Nanosecond, Some(Arc::from("UTC"))),
            false,
        )]);

        let schema_adapter =
            DefaultSchemaAdapterFactory {}.create(Arc::new(table_schema.clone()));
        let (schema_mapping, _) = schema_adapter
            .map_schema(&file_schema)
            .expect("creating schema mapping");

        let mut parquet_reader = parquet_reader_builder.build().expect("building reader");

        // Parquet file is small, we only need 1 recordbatch
        let first_rb = parquet_reader
            .next()
            .expect("expected record batch")
            .expect("expected error free record batch");

        // Test all should fail
        let expr = col("timestamp_col").lt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(1), Some(Arc::from("UTC"))),
        ));
        let expr = logical2physical(&expr, &table_schema);
        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &file_schema,
            &metadata,
            Count::new(),
            Time::new(),
            Arc::clone(&schema_mapping),
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb.clone());
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![false; 8])));

        // Test all should pass
        let expr = col("timestamp_col").gt(Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(0), Some(Arc::from("UTC"))),
        ));
        let expr = logical2physical(&expr, &table_schema);
        let candidate = FilterCandidateBuilder::new(expr, &file_schema, &table_schema)
            .build(&metadata)
            .expect("building candidate")
            .expect("candidate expected");

        let mut row_filter = DatafusionArrowPredicate::try_new(
            candidate,
            &file_schema,
            &metadata,
            Count::new(),
            Time::new(),
            schema_mapping,
        )
        .expect("creating filter predicate");

        let filtered = row_filter.evaluate(first_rb);
        assert!(matches!(filtered, Ok(a) if a == BooleanArray::from(vec![true; 8])));
    }

    #[test]
    fn test_remap_projection() {
        let mut rng = thread_rng();
        for _ in 0..100 {
            // A random selection of column indexes in arbitrary order
            let projection: Vec<_> = (0..100).map(|_| rng.gen()).collect();

            // File order is the projection sorted
            let mut file_order = projection.clone();
            file_order.sort_unstable();

            let remap = remap_projection(&projection);
            // Applying the remapped projection to the file order should yield the original
            let remapped: Vec<_> = remap.iter().map(|r| file_order[*r]).collect();
            assert_eq!(projection, remapped)
        }
    }
}
