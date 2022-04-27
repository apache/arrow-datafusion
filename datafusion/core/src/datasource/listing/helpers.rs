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

use std::path::{Component, Path};
use std::sync::Arc;

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayRef, Date64Array, Date64Builder, StringArray,
        StringBuilder, UInt64Array, UInt64Builder,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::{TimeZone, Utc};
use datafusion_common::DataFusionError;
use futures::{
    stream::{self},
    StreamExt, TryStreamExt,
};
use log::debug;

use crate::{
    datasource::MemTable,
    error::Result,
    execution::context::SessionContext,
    logical_plan::{self, Expr, ExprVisitable, ExpressionVisitor, Recursion},
    scalar::ScalarValue,
};

use super::{PartitionedFile, PartitionedFileStream};
use datafusion_data_access::{object_store::ObjectStore, FileMeta, SizedFile};
use datafusion_expr::Volatility;

const FILE_SIZE_COLUMN_NAME: &str = "_df_part_file_size_";
const FILE_PATH_COLUMN_NAME: &str = "_df_part_file_path_";
const FILE_MODIFIED_COLUMN_NAME: &str = "_df_part_file_modified_";

/// The `ExpressionVisitor` for `expr_applicable_for_cols`. Walks the tree to
/// validate that the given expression is applicable with only the `col_names`
/// set of columns.
struct ApplicabilityVisitor<'a> {
    col_names: &'a [String],
    is_applicable: &'a mut bool,
}

impl ApplicabilityVisitor<'_> {
    fn visit_volatility(self, volatility: Volatility) -> Recursion<Self> {
        match volatility {
            Volatility::Immutable => Recursion::Continue(self),
            // TODO: Stable functions could be `applicable`, but that would require access to the context
            Volatility::Stable | Volatility::Volatile => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        }
    }
}

impl ExpressionVisitor for ApplicabilityVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        let rec = match expr {
            Expr::Column(logical_plan::Column { ref name, .. }) => {
                *self.is_applicable &= self.col_names.contains(name);
                Recursion::Stop(self) // leaf node anyway
            }
            Expr::Literal(_)
            | Expr::Alias(_, _)
            | Expr::ScalarVariable(_, _)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::GetIndexedField { .. }
            | Expr::Case { .. } => Recursion::Continue(self),

            Expr::ScalarFunction { fun, .. } => self.visit_volatility(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => {
                self.visit_volatility(fun.signature.volatility)
            }
            Expr::TableUDF { fun, .. } => self.visit_volatility(fun.signature.volatility),

            // TODO other expressions are not handled yet:
            // - AGGREGATE, WINDOW and SORT should not end up in filter conditions, except maybe in some edge cases
            // - Can `Wildcard` be considered as a `Literal`?
            // - ScalarVariable could be `applicable`, but that would require access to the context
            Expr::AggregateUDF { .. }
            | Expr::AggregateFunction { .. }
            | Expr::Sort { .. }
            | Expr::WindowFunction { .. }
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        };
        Ok(rec)
    }
}

/// Check whether the given expression can be resolved using only the columns `col_names`.
/// This means that if this function returns true:
/// - the table provider can filter the table partition values with this expression
/// - the expression can be marked as `TableProviderFilterPushDown::Exact` once this filtering
/// was performed
pub fn expr_applicable_for_cols(col_names: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.accept(ApplicabilityVisitor {
        col_names,
        is_applicable: &mut is_applicable,
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
pub async fn pruned_partition_list(
    store: &dyn ObjectStore,
    table_path: &str,
    filters: &[Expr],
    file_extension: &str,
    table_partition_cols: &[String],
) -> Result<PartitionedFileStream> {
    // if no partition col => simply list all the files
    if table_partition_cols.is_empty() {
        return Ok(Box::pin(
            store
                .list_file_with_suffix(table_path, file_extension)
                .await?
                .map(|f| {
                    Ok(PartitionedFile {
                        partition_values: vec![],
                        file_meta: f?,
                        range: None,
                    })
                }),
        ));
    }

    let applicable_filters: Vec<_> = filters
        .iter()
        .filter(|f| expr_applicable_for_cols(table_partition_cols, f))
        .collect();
    let stream_path = table_path.to_owned();
    if applicable_filters.is_empty() {
        // Parse the partition values while listing all the files
        // Note: We might avoid parsing the partition values if they are not used in any projection,
        // but the cost of parsing will likely be far dominated by the time to fetch the listing from
        // the object store.
        let table_partition_cols_stream = table_partition_cols.to_vec();
        Ok(Box::pin(
            store
                .list_file_with_suffix(table_path, file_extension)
                .await?
                .filter_map(move |f| {
                    let stream_path = stream_path.clone();
                    let table_partition_cols_stream = table_partition_cols_stream.clone();
                    async move {
                        let file_meta = match f {
                            Ok(fm) => fm,
                            Err(err) => return Some(Err(err)),
                        };
                        let parsed_path = parse_partitions_for_path(
                            &stream_path,
                            file_meta.path(),
                            &table_partition_cols_stream,
                        )
                        .map(|p| {
                            p.iter()
                                .map(|&pn| ScalarValue::Utf8(Some(pn.to_owned())))
                                .collect()
                        });

                        parsed_path.map(|partition_values| {
                            Ok(PartitionedFile {
                                partition_values,
                                file_meta,
                                range: None,
                            })
                        })
                    }
                }),
        ))
    } else {
        // parse the partition values and serde them as a RecordBatch to filter them
        // TODO avoid collecting but have a streaming memory table instead
        let batches: Vec<RecordBatch> = store
            .list_file_with_suffix(table_path, file_extension)
            .await?
            // TODO we set an arbitrary high batch size here, it does not matter as we list
            // all the files anyway. This number will need to be adjusted according to the object
            // store if we switch to a streaming-stlye pruning of the files. For instance S3 lists
            // 1000 items at a time so batches of 1000 would be ideal with S3 as store.
            .chunks(1024)
            .map(|v| {
                v.into_iter()
                    .collect::<datafusion_data_access::Result<Vec<_>>>()
            })
            .map_err(DataFusionError::IoError)
            .map(move |metas| paths_to_batch(table_partition_cols, &stream_path, &metas?))
            .try_collect()
            .await?;

        let mem_table = MemTable::try_new(batches[0].schema(), vec![batches])?;

        // Filter the partitions using a local datafusion context
        // TODO having the external context would allow us to resolve `Volatility::Stable`
        // scalar functions (`ScalarFunction` & `ScalarUDF`) and `ScalarVariable`s
        let ctx = SessionContext::new();
        let mut df = ctx.read_table(Arc::new(mem_table))?;
        for filter in applicable_filters {
            df = df.filter(filter.clone())?;
        }
        let filtered_batches = df.collect().await?;

        Ok(Box::pin(stream::iter(
            batches_to_paths(&filtered_batches).into_iter().map(Ok),
        )))
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
    table_partition_cols: &[String],
    table_path: &str,
    metas: &[FileMeta],
) -> Result<RecordBatch> {
    let mut key_builder = StringBuilder::new(metas.len());
    let mut length_builder = UInt64Builder::new(metas.len());
    let mut modified_builder = Date64Builder::new(metas.len());
    let mut partition_builders = table_partition_cols
        .iter()
        .map(|_| StringBuilder::new(metas.len()))
        .collect::<Vec<_>>();
    for file_meta in metas {
        if let Some(partition_values) =
            parse_partitions_for_path(table_path, file_meta.path(), table_partition_cols)
        {
            key_builder.append_value(file_meta.path())?;
            length_builder.append_value(file_meta.size())?;
            match file_meta.last_modified {
                Some(lm) => modified_builder.append_value(lm.timestamp_millis())?,
                None => modified_builder.append_null()?,
            }
            for (i, part_val) in partition_values.iter().enumerate() {
                partition_builders[i].append_value(part_val)?;
            }
        } else {
            debug!("No partitioning for path {}", file_meta.path());
        }
    }

    // finish all builders
    let mut col_arrays: Vec<ArrayRef> = vec![
        ArrayBuilder::finish(&mut key_builder),
        ArrayBuilder::finish(&mut length_builder),
        ArrayBuilder::finish(&mut modified_builder),
    ];
    for mut partition_builder in partition_builders {
        col_arrays.push(ArrayBuilder::finish(&mut partition_builder));
    }

    // put the schema together
    let mut fields = vec![
        Field::new(FILE_PATH_COLUMN_NAME, DataType::Utf8, false),
        Field::new(FILE_SIZE_COLUMN_NAME, DataType::UInt64, false),
        Field::new(FILE_MODIFIED_COLUMN_NAME, DataType::Date64, false),
    ];
    for pn in table_partition_cols {
        fields.push(Field::new(pn, DataType::Utf8, false));
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), col_arrays)?;
    Ok(batch)
}

/// convert a set of record batches created by `paths_to_batch()` back to partitioned files.
fn batches_to_paths(batches: &[RecordBatch]) -> Vec<PartitionedFile> {
    batches
        .iter()
        .flat_map(|batch| {
            let key_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let length_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let modified_array = batch
                .column(2)
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap();

            (0..batch.num_rows()).map(move |row| PartitionedFile {
                file_meta: FileMeta {
                    last_modified: match modified_array.is_null(row) {
                        false => Some(Utc.timestamp_millis(modified_array.value(row))),
                        true => None,
                    },
                    sized_file: SizedFile {
                        path: key_array.value(row).to_owned(),
                        size: length_array.value(row),
                    },
                },
                partition_values: (3..batch.columns().len())
                    .map(|col| {
                        ScalarValue::try_from_array(batch.column(col), row).unwrap()
                    })
                    .collect(),
                range: None,
            })
        })
        .collect()
}

/// Extract the partition values for the given `file_path` (in the given `table_path`)
/// associated to the partitions defined by `table_partition_cols`
fn parse_partitions_for_path<'a>(
    table_path: &str,
    file_path: &'a str,
    table_partition_cols: &[String],
) -> Option<Vec<&'a str>> {
    let subpath = file_path.strip_prefix(table_path)?;

    // split subpath into components ignoring leading separator if exists
    let subpath = Path::new(subpath)
        .components()
        .filter(|c| !matches!(c, Component::RootDir))
        .filter_map(|c| c.as_os_str().to_str());

    let mut part_values = vec![];
    for (path, pn) in subpath.zip(table_partition_cols) {
        match path.split_once('=') {
            Some((name, val)) if name == pn => part_values.push(val),
            _ => return None,
        }
    }
    Some(part_values)
}

#[cfg(test)]
mod tests {
    use crate::{
        logical_plan::{case, col, lit},
        test::object_store::TestObjectStore,
    };

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
        let store = TestObjectStore::new_arc(&[
            ("tablepath/mypartition=val1/notparquetfile", 100),
            ("tablepath/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            store.as_ref(),
            "tablepath/",
            &[filter],
            ".parquet",
            &[String::from("mypartition")],
        )
        .await
        .expect("partition pruning failed")
        .collect::<Vec<_>>()
        .await;

        assert_eq!(pruned.len(), 0);
    }

    #[tokio::test]
    async fn test_pruned_partition_list() {
        let store = TestObjectStore::new_arc(&[
            ("tablepath/mypartition=val1/file.parquet", 100),
            ("tablepath/mypartition=val2/file.parquet", 100),
            ("tablepath/mypartition=val1/other=val3/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            store.as_ref(),
            "tablepath/",
            &[filter],
            ".parquet",
            &[String::from("mypartition")],
        )
        .await
        .expect("partition pruning failed")
        .collect::<Vec<_>>()
        .await;

        assert_eq!(pruned.len(), 2);
        let f1 = pruned[0].as_ref().expect("first item not an error");
        assert_eq!(
            &f1.file_meta.sized_file.path,
            "tablepath/mypartition=val1/file.parquet"
        );
        assert_eq!(
            &f1.partition_values,
            &[ScalarValue::Utf8(Some(String::from("val1"))),]
        );
        let f2 = pruned[1].as_ref().expect("second item not an error");
        assert_eq!(
            &f2.file_meta.sized_file.path,
            "tablepath/mypartition=val1/other=val3/file.parquet"
        );
        assert_eq!(
            &f2.partition_values,
            &[ScalarValue::Utf8(Some(String::from("val1"))),]
        );
    }

    #[tokio::test]
    async fn test_pruned_partition_list_multi() {
        let store = TestObjectStore::new_arc(&[
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
            "tablepath/",
            &[filter1, filter2, filter3],
            ".parquet",
            &[String::from("part1"), String::from("part2")],
        )
        .await
        .expect("partition pruning failed")
        .collect::<Vec<_>>()
        .await;

        assert_eq!(pruned.len(), 2);
        let f1 = pruned[0].as_ref().expect("first item not an error");
        assert_eq!(
            &f1.file_meta.sized_file.path,
            "tablepath/part1=p1v2/part2=p2v1/file1.parquet"
        );
        assert_eq!(
            &f1.partition_values,
            &[
                ScalarValue::Utf8(Some(String::from("p1v2"))),
                ScalarValue::Utf8(Some(String::from("p2v1")))
            ]
        );
        let f2 = pruned[1].as_ref().expect("second item not an error");
        assert_eq!(
            &f2.file_meta.sized_file.path,
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
            parse_partitions_for_path("bucket/mytable", "bucket/mytable/file.csv", &[])
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                "bucket/othertable",
                "bucket/mytable/file.csv",
                &[]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                "bucket/mytable",
                "bucket/mytable/file.csv",
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                "bucket/mytable",
                "bucket/mytable/mypartition=v1/file.csv",
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                "bucket/mytable/",
                "bucket/mytable/mypartition=v1/file.csv",
                &[String::from("mypartition")]
            )
        );
        // Only hive style partitioning supported for now:
        assert_eq!(
            None,
            parse_partitions_for_path(
                "bucket/mytable",
                "bucket/mytable/v1/file.csv",
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1", "v2"]),
            parse_partitions_for_path(
                "bucket/mytable",
                "bucket/mytable/mypartition=v1/otherpartition=v2/file.csv",
                &[String::from("mypartition"), String::from("otherpartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                "bucket/mytable",
                "bucket/mytable/mypartition=v1/otherpartition=v2/file.csv",
                &[String::from("mypartition")]
            )
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_parse_partitions_for_path_windows() {
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                "bucket\\mytable",
                "bucket\\mytable\\mypartition=v1\\file.csv",
                &[String::from("mypartition")]
            )
        );
        assert_eq!(
            Some(vec!["v1", "v2"]),
            parse_partitions_for_path(
                "bucket\\mytable",
                "bucket\\mytable\\mypartition=v1\\otherpartition=v2\\file.csv",
                &[String::from("mypartition"), String::from("otherpartition")]
            )
        );
    }

    #[test]
    fn test_path_batch_roundtrip_no_partiton() {
        let files = vec![
            FileMeta {
                sized_file: SizedFile {
                    path: String::from("mybucket/tablepath/part1=val1/file.parquet"),
                    size: 100,
                },
                last_modified: Some(Utc.timestamp_millis(1634722979123)),
            },
            FileMeta {
                sized_file: SizedFile {
                    path: String::from("mybucket/tablepath/part1=val2/file.parquet"),
                    size: 100,
                },
                last_modified: None,
            },
        ];

        let batches = paths_to_batch(&[], "mybucket/tablepath", &files)
            .expect("Serialization of file list to batch failed");

        let parsed_files = batches_to_paths(&[batches]);
        assert_eq!(parsed_files.len(), 2);
        assert_eq!(&parsed_files[0].partition_values, &[]);
        assert_eq!(&parsed_files[1].partition_values, &[]);

        let parsed_metas = parsed_files
            .into_iter()
            .map(|pf| pf.file_meta)
            .collect::<Vec<_>>();
        assert_eq!(parsed_metas, files);
    }

    #[test]
    fn test_path_batch_roundtrip_with_partition() {
        let files = vec![
            FileMeta {
                sized_file: SizedFile {
                    path: String::from("mybucket/tablepath/part1=val1/file.parquet"),
                    size: 100,
                },
                last_modified: Some(Utc.timestamp_millis(1634722979123)),
            },
            FileMeta {
                sized_file: SizedFile {
                    path: String::from("mybucket/tablepath/part1=val2/file.parquet"),
                    size: 100,
                },
                last_modified: None,
            },
        ];

        let batches =
            paths_to_batch(&[String::from("part1")], "mybucket/tablepath", &files)
                .expect("Serialization of file list to batch failed");

        let parsed_files = batches_to_paths(&[batches]);
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
            .map(|pf| pf.file_meta)
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
