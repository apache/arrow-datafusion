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

//! Helper functions for the table implementation

use std::sync::Arc;

use arrow::array::new_empty_array;
use arrow::{
    array::{ArrayBuilder, ArrayRef, Date64Builder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{TimeZone, Utc};
use futures::{stream::BoxStream, TryStreamExt};
use log::debug;

use crate::{
    datasource::MemTable, error::Result, execution::context::SessionContext,
    scalar::ScalarValue,
};

use super::PartitionedFile;
use crate::datasource::listing::ListingTableUrl;
use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use datafusion_common::{
    cast::{as_date64_array, as_string_array, as_uint64_array},
    Column, DataFusionError,
};
use datafusion_expr::{Expr, Volatility};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

const FILE_SIZE_COLUMN_NAME: &str = "_df_part_file_size_";
const FILE_PATH_COLUMN_NAME: &str = "_df_part_file_path_";
const FILE_MODIFIED_COLUMN_NAME: &str = "_df_part_file_modified_";

/// Check whether the given expression can be resolved using only the columns `col_names`.
/// This means that if this function returns true:
/// - the table provider can filter the table partition values with this expression
/// - the expression can be marked as `TableProviderFilterPushDown::Exact` once this filtering
/// was performed
pub fn expr_applicable_for_cols(col_names: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.apply(&mut |expr| {
        Ok(match expr {
            Expr::Column(Column { ref name, .. }) => {
                is_applicable &= col_names.contains(name);
                if is_applicable {
                    VisitRecursion::Skip
                } else {
                    VisitRecursion::Stop
                }
            }
            Expr::Literal(_)
            | Expr::Alias(_, _)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::HiddenColumn(_, _)
            | Expr::HiddenExpr(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Between { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::SimilarTo { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::GetIndexedField { .. }
            | Expr::GroupingSet(_)
            | Expr::Case { .. } => VisitRecursion::Continue,

            Expr::ScalarFunction { fun, .. } => {
                match fun.volatility() {
                    Volatility::Immutable => VisitRecursion::Continue,
                    // TODO: Stable functions could be `applicable`, but that would require access to the context
                    Volatility::Stable | Volatility::Volatile => {
                        is_applicable = false;
                        VisitRecursion::Stop
                    }
                }
            }
            Expr::ScalarUDF { fun, .. } => {
                match fun.signature.volatility {
                    Volatility::Immutable => VisitRecursion::Continue,
                    // TODO: Stable functions could be `applicable`, but that would require access to the context
                    Volatility::Stable | Volatility::Volatile => {
                        is_applicable = false;
                        VisitRecursion::Stop
                    }
                }
            }

            // TODO other expressions are not handled yet:
            // - AGGREGATE, WINDOW and SORT should not end up in filter conditions, except maybe in some edge cases
            // - Can `Wildcard` be considered as a `Literal`?
            // - ScalarVariable could be `applicable`, but that would require access to the context
            Expr::AggregateUDF { .. }
            | Expr::AggregateFunction { .. }
            | Expr::Sort { .. }
            | Expr::WindowFunction { .. }
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::Placeholder { .. } => {
                is_applicable = false;
                VisitRecursion::Stop
            }
        })
    })
    .unwrap();
    is_applicable
}

/// Partition the list of files into `n` groups
pub fn split_files(
    partitioned_files: Vec<PartitionedFile>,
    n: usize,
) -> Vec<Vec<PartitionedFile>> {
    if partitioned_files.is_empty() {
        return vec![];
    }
    // effectively this is div with rounding up instead of truncating
    let chunk_size = (partitioned_files.len() + n - 1) / n;
    partitioned_files
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect()
}

/// Discover the partitions on the given path and prune out files
/// that belong to irrelevant partitions using `filters` expressions.
/// `filters` might contain expressions that can be resolved only at the
/// file level (e.g. Parquet row group pruning).
///
/// TODO for tables with many files (10k+), it will usually more efficient
/// to first list the folders relative to the first partition dimension,
/// prune those, then list only the contain of the remaining folders.
pub async fn pruned_partition_list<'a>(
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    table_partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    let list = table_path.list_all_files(store, file_extension);

    // if no partition col => simply list all the files
    if table_partition_cols.is_empty() {
        return Ok(Box::pin(list.map_ok(|object_meta| object_meta.into())));
    }

    let applicable_filters: Vec<_> = filters
        .iter()
        .filter(|f| {
            expr_applicable_for_cols(
                &table_partition_cols
                    .iter()
                    .map(|x| x.0.clone())
                    .collect::<Vec<_>>(),
                f,
            )
        })
        .collect();

    if applicable_filters.is_empty() {
        // Parse the partition values while listing all the files
        // Note: We might avoid parsing the partition values if they are not used in any projection,
        // but the cost of parsing will likely be far dominated by the time to fetch the listing from
        // the object store.
        Ok(Box::pin(list.try_filter_map(
            move |object_meta| async move {
                let parsed_path = parse_partitions_for_path(
                    table_path,
                    &object_meta.location,
                    &table_partition_cols
                        .iter()
                        .map(|x| x.0.clone())
                        .collect::<Vec<_>>(),
                )
                .map(|p| {
                    p.iter()
                        .zip(table_partition_cols)
                        .map(|(&part_value, part_column)| {
                            ScalarValue::try_from_string(
                                part_value.to_string(),
                                &part_column.1,
                            )
                            .unwrap_or_else(|_| {
                                panic!(
                                    "Failed to cast str {} to type {}",
                                    part_value, part_column.1
                                )
                            })
                        })
                        .collect()
                });

                Ok(parsed_path.map(|partition_values| PartitionedFile {
                    partition_values,
                    object_meta,
                    range: None,
                    extensions: None,
                }))
            },
        )))
    } else {
        // parse the partition values and serde them as a RecordBatch to filter them
        let metas: Vec<_> = list.try_collect().await?;
        let batch = paths_to_batch(table_partition_cols, table_path, &metas)?;
        let mem_table = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
        debug!("get mem_table: {:?}", mem_table);

        // Filter the partitions using a local datafusion context
        // TODO having the external context would allow us to resolve `Volatility::Stable`
        // scalar functions (`ScalarFunction` & `ScalarUDF`) and `ScalarVariable`s
        let ctx = SessionContext::new();
        let mut df = ctx.read_table(Arc::new(mem_table))?;
        for filter in applicable_filters {
            df = df.filter(filter.clone())?;
        }
        let filtered_batches = df.collect().await?;
        let paths = batches_to_paths(&filtered_batches)?;

        Ok(Box::pin(futures::stream::iter(paths.into_iter().map(Ok))))
    }
}

/// convert the paths of the files to a record batch with the following columns:
/// - one column for the file size named `_df_part_file_size_`
/// - one column for with the original path named `_df_part_file_path_`
/// - one column for with the last modified date named `_df_part_file_modified_`
/// - ... one column by partition ...
///
/// Note: For the last modified date, this looses precisions higher than millisecond.
fn paths_to_batch(
    table_partition_cols: &[(String, DataType)],
    table_path: &ListingTableUrl,
    metas: &[ObjectMeta],
) -> Result<RecordBatch> {
    let mut key_builder = StringBuilder::with_capacity(metas.len(), 1024);
    let mut length_builder = UInt64Builder::with_capacity(metas.len());
    let mut modified_builder = Date64Builder::with_capacity(metas.len());
    let mut partition_scalar_values = table_partition_cols
        .iter()
        .map(|_| Vec::new())
        .collect::<Vec<_>>();
    for file_meta in metas {
        if let Some(partition_values) = parse_partitions_for_path(
            table_path,
            &file_meta.location,
            &table_partition_cols
                .iter()
                .map(|x| x.0.clone())
                .collect::<Vec<_>>(),
        ) {
            key_builder.append_value(file_meta.location.as_ref());
            length_builder.append_value(file_meta.size as u64);
            modified_builder.append_value(file_meta.last_modified.timestamp_millis());
            for (i, part_val) in partition_values.iter().enumerate() {
                let scalar_val = ScalarValue::try_from_string(
                    part_val.to_string(),
                    &table_partition_cols[i].1,
                )?;
                partition_scalar_values[i].push(scalar_val);
            }
        } else {
            debug!("No partitioning for path {}", file_meta.location);
        }
    }

    // finish all builders
    let mut col_arrays: Vec<ArrayRef> = vec![
        ArrayBuilder::finish(&mut key_builder),
        ArrayBuilder::finish(&mut length_builder),
        ArrayBuilder::finish(&mut modified_builder),
    ];
    for (i, part_scalar_val) in partition_scalar_values.into_iter().enumerate() {
        if part_scalar_val.is_empty() {
            col_arrays.push(new_empty_array(&table_partition_cols[i].1));
        } else {
            let partition_val_array = ScalarValue::iter_to_array(part_scalar_val)?;
            col_arrays.push(partition_val_array);
        }
    }

    // put the schema together
    let mut fields = vec![
        Field::new(FILE_PATH_COLUMN_NAME, DataType::Utf8, false),
        Field::new(FILE_SIZE_COLUMN_NAME, DataType::UInt64, false),
        Field::new(FILE_MODIFIED_COLUMN_NAME, DataType::Date64, true),
    ];
    for part_col in table_partition_cols {
        fields.push(Field::new(&part_col.0, part_col.1.to_owned(), false));
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), col_arrays)?;
    Ok(batch)
}

/// convert a set of record batches created by `paths_to_batch()` back to partitioned files.
fn batches_to_paths(batches: &[RecordBatch]) -> Result<Vec<PartitionedFile>> {
    batches
        .iter()
        .flat_map(|batch| {
            let key_array = as_string_array(batch.column(0)).unwrap();
            let length_array = as_uint64_array(batch.column(1)).unwrap();
            let modified_array = as_date64_array(batch.column(2)).unwrap();

            (0..batch.num_rows()).map(move |row| {
                Ok(PartitionedFile {
                    object_meta: ObjectMeta {
                        location: Path::parse(key_array.value(row))
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                        last_modified: to_timestamp_millis(modified_array.value(row))?,
                        size: length_array.value(row) as usize,
                    },
                    partition_values: (3..batch.columns().len())
                        .map(|col| {
                            ScalarValue::try_from_array(batch.column(col), row).unwrap()
                        })
                        .collect(),
                    range: None,
                    extensions: None,
                })
            })
        })
        .collect()
}

fn to_timestamp_millis(v: i64) -> Result<chrono::DateTime<Utc>> {
    match Utc.timestamp_millis_opt(v) {
        chrono::LocalResult::None => Err(DataFusionError::Execution(format!(
            "Can not convert {v} to UTC millisecond timestamp"
        ))),
        chrono::LocalResult::Single(v) => Ok(v),
        chrono::LocalResult::Ambiguous(_, _) => Err(DataFusionError::Execution(format!(
            "Ambiguous timestamp when converting {v} to UTC millisecond timestamp"
        ))),
    }
}

/// Extract the partition values for the given `file_path` (in the given `table_path`)
/// associated to the partitions defined by `table_partition_cols`
fn parse_partitions_for_path<'a>(
    table_path: &ListingTableUrl,
    file_path: &'a Path,
    table_partition_cols: &[String],
) -> Option<Vec<&'a str>> {
    let subpath = table_path.strip_prefix(file_path)?;

    let mut part_values = vec![];
    for (part, pn) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == pn => part_values.push(val),
            _ => return None,
        }
    }
    Some(part_values)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use crate::logical_expr::{case, col, lit};
    use crate::test::object_store::make_test_store;

    use super::*;

    #[test]
    fn test_split_files() {
        let new_partitioned_file = |path: &str| PartitionedFile::new(path.to_owned(), 10);
        let files = vec![
            new_partitioned_file("a"),
            new_partitioned_file("b"),
            new_partitioned_file("c"),
            new_partitioned_file("d"),
            new_partitioned_file("e"),
        ];

        let chunks = split_files(files.clone(), 1);
        assert_eq!(1, chunks.len());
        assert_eq!(5, chunks[0].len());

        let chunks = split_files(files.clone(), 2);
        assert_eq!(2, chunks.len());
        assert_eq!(3, chunks[0].len());
        assert_eq!(2, chunks[1].len());

        let chunks = split_files(files.clone(), 5);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(files, 123);
        assert_eq!(5, chunks.len());
        assert_eq!(1, chunks[0].len());
        assert_eq!(1, chunks[1].len());
        assert_eq!(1, chunks[2].len());
        assert_eq!(1, chunks[3].len());
        assert_eq!(1, chunks[4].len());

        let chunks = split_files(vec![], 2);
        assert_eq!(0, chunks.len());
    }

    #[tokio::test]
    async fn test_pruned_partition_list_empty() {
        let store = make_test_store(&[
            ("tablepath/mypartition=val1/notparquetfile", 100),
            ("tablepath/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            store.as_ref(),
            &ListingTableUrl::parse("file:///tablepath/").unwrap(),
            &[filter],
            ".parquet",
            &[(String::from("mypartition"), DataType::Utf8)],
        )
        .await
        .expect("partition pruning failed")
        .collect::<Vec<_>>()
        .await;

        assert_eq!(pruned.len(), 0);
    }

    #[tokio::test]
    async fn test_pruned_partition_list() {
        let store = make_test_store(&[
            ("tablepath/mypartition=val1/file.parquet", 100),
            ("tablepath/mypartition=val2/file.parquet", 100),
            ("tablepath/mypartition=val1/other=val3/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            store.as_ref(),
            &ListingTableUrl::parse("file:///tablepath/").unwrap(),
            &[filter],
            ".parquet",
            &[(String::from("mypartition"), DataType::Utf8)],
        )
        .await
        .expect("partition pruning failed")
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(pruned.len(), 2);
        let f1 = &pruned[0];
        assert_eq!(
            f1.object_meta.location.as_ref(),
            "tablepath/mypartition=val1/file.parquet"
        );
        assert_eq!(
            &f1.partition_values,
            &[ScalarValue::Utf8(Some(String::from("val1"))),]
        );
        let f2 = &pruned[1];
        assert_eq!(
            f2.object_meta.location.as_ref(),
            "tablepath/mypartition=val1/other=val3/file.parquet"
        );
        assert_eq!(
            f2.partition_values,
            &[ScalarValue::Utf8(Some(String::from("val1"))),]
        );
    }

    #[tokio::test]
    async fn test_pruned_partition_list_multi() {
        let store = make_test_store(&[
            ("tablepath/part1=p1v1/file.parquet", 100),
            ("tablepath/part1=p1v2/part2=p2v1/file1.parquet", 100),
            ("tablepath/part1=p1v2/part2=p2v1/file2.parquet", 100),
            ("tablepath/part1=p1v3/part2=p2v1/file2.parquet", 100),
            ("tablepath/part1=p1v2/part2=p2v2/file2.parquet", 100),
        ]);
        let filter1 = Expr::eq(col("part1"), lit("p1v2"));
        let filter2 = Expr::eq(col("part2"), lit("p2v1"));
        // filter3 cannot be resolved at partition pruning
        let filter3 = Expr::eq(col("part2"), col("other"));
        let pruned = pruned_partition_list(
            store.as_ref(),
            &ListingTableUrl::parse("file:///tablepath/").unwrap(),
            &[filter1, filter2, filter3],
            ".parquet",
            &[
                (String::from("part1"), DataType::Utf8),
                (String::from("part2"), DataType::Utf8),
            ],
        )
        .await
        .expect("partition pruning failed")
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

        assert_eq!(pruned.len(), 2);
        let f1 = &pruned[0];
        assert_eq!(
            f1.object_meta.location.as_ref(),
            "tablepath/part1=p1v2/part2=p2v1/file1.parquet"
        );
        assert_eq!(
            &f1.partition_values,
            &[
                ScalarValue::Utf8(Some(String::from("p1v2"))),
                ScalarValue::Utf8(Some(String::from("p2v1")))
            ]
        );
        let f2 = &pruned[1];
        assert_eq!(
            f2.object_meta.location.as_ref(),
            "tablepath/part1=p1v2/part2=p2v1/file2.parquet"
        );
        assert_eq!(
            &f2.partition_values,
            &[
                ScalarValue::Utf8(Some(String::from("p1v2"))),
                ScalarValue::Utf8(Some(String::from("p2v1")))
            ]
        );
    }

    #[test]
    fn test_parse_partitions_for_path() {
        assert_eq!(
            Some(vec![]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                &[]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/othertable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                &[]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable/").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                &[String::from("mypartition")]
            )
        );
        // Only hive style partitioning supported for now:
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/v1/file.csv"),
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1", "v2"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                &[String::from("mypartition"), String::from("otherpartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                &[String::from("mypartition")]
            )
        );
    }

    #[test]
    fn test_path_batch_roundtrip_no_partiton() {
        let files = vec![
            ObjectMeta {
                location: Path::from("mybucket/tablepath/part1=val1/file.parquet"),
                last_modified: to_timestamp_millis(1634722979123).unwrap(),
                size: 100,
            },
            ObjectMeta {
                location: Path::from("mybucket/tablepath/part1=val2/file.parquet"),
                last_modified: to_timestamp_millis(0).unwrap(),
                size: 100,
            },
        ];

        let table_path = ListingTableUrl::parse("file:///mybucket/tablepath").unwrap();
        let batches = paths_to_batch(&[], &table_path, &files)
            .expect("Serialization of file list to batch failed");

        let parsed_files = batches_to_paths(&[batches]).unwrap();
        assert_eq!(parsed_files.len(), 2);
        assert_eq!(&parsed_files[0].partition_values, &[]);
        assert_eq!(&parsed_files[1].partition_values, &[]);

        let parsed_metas = parsed_files
            .into_iter()
            .map(|pf| pf.object_meta)
            .collect::<Vec<_>>();
        assert_eq!(parsed_metas, files);
    }

    #[test]
    fn test_path_batch_roundtrip_with_partition() {
        let files = vec![
            ObjectMeta {
                location: Path::from("mybucket/tablepath/part1=val1/file.parquet"),
                last_modified: to_timestamp_millis(1634722979123).unwrap(),
                size: 100,
            },
            ObjectMeta {
                location: Path::from("mybucket/tablepath/part1=val2/file.parquet"),
                last_modified: to_timestamp_millis(0).unwrap(),
                size: 100,
            },
        ];

        let batches = paths_to_batch(
            &[(String::from("part1"), DataType::Utf8)],
            &ListingTableUrl::parse("file:///mybucket/tablepath").unwrap(),
            &files,
        )
        .expect("Serialization of file list to batch failed");

        let parsed_files = batches_to_paths(&[batches]).unwrap();
        assert_eq!(parsed_files.len(), 2);
        assert_eq!(
            &parsed_files[0].partition_values,
            &[ScalarValue::Utf8(Some(String::from("val1")))]
        );
        assert_eq!(
            &parsed_files[1].partition_values,
            &[ScalarValue::Utf8(Some(String::from("val2")))]
        );

        let parsed_metas = parsed_files
            .into_iter()
            .map(|pf| pf.object_meta)
            .collect::<Vec<_>>();
        assert_eq!(parsed_metas, files);
    }

    #[test]
    fn test_expr_applicable_for_cols() {
        assert!(expr_applicable_for_cols(
            &[String::from("c1")],
            &Expr::eq(col("c1"), lit("value"))
        ));
        assert!(!expr_applicable_for_cols(
            &[String::from("c1")],
            &Expr::eq(col("c2"), lit("value"))
        ));
        assert!(!expr_applicable_for_cols(
            &[String::from("c1")],
            &Expr::eq(col("c1"), col("c2"))
        ));
        assert!(expr_applicable_for_cols(
            &[String::from("c1"), String::from("c2")],
            &Expr::eq(col("c1"), col("c2"))
        ));
        assert!(expr_applicable_for_cols(
            &[String::from("c1"), String::from("c2")],
            &(Expr::eq(col("c1"), col("c2").alias("c2_alias"))).not()
        ));
        assert!(expr_applicable_for_cols(
            &[String::from("c1"), String::from("c2")],
            &(case(col("c1"))
                .when(lit("v1"), lit(true))
                .otherwise(lit(false))
                .expect("valid case expr"))
        ));
        // static expression not relvant in this context but we
        // test it as an edge case anyway in case we want to generalize
        // this helper function
        assert!(expr_applicable_for_cols(&[], &lit(true)));
    }
}
