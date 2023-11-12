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

use arrow::compute::{and, cast, prep_null_mask_filter};
use arrow::{
    array::{ArrayRef, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_array::cast::AsArray;
use arrow_array::Array;
use arrow_schema::Fields;
use futures::stream::FuturesUnordered;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use log::{debug, trace};

use crate::{error::Result, scalar::ScalarValue};

use super::PartitionedFile;
use crate::datasource::listing::ListingTableUrl;
use crate::execution::context::SessionState;
use datafusion_common::tree_node::{TreeNode, VisitRecursion};
use datafusion_common::{Column, DFField, DFSchema, DataFusionError};
use datafusion_expr::expr::ScalarUDF;
use datafusion_expr::{Expr, Volatility};
use datafusion_physical_expr::create_physical_expr;
use datafusion_physical_expr::execution_props::ExecutionProps;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

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
            | Expr::Alias(_)
            | Expr::OuterReferenceColumn(_, _)
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
            | Expr::SimilarTo { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::GetIndexedField { .. }
            | Expr::GroupingSet(_)
            | Expr::Case { .. } => VisitRecursion::Continue,

            Expr::ScalarFunction(scalar_function) => {
                match scalar_function.fun.volatility() {
                    Volatility::Immutable => VisitRecursion::Continue,
                    // TODO: Stable functions could be `applicable`, but that would require access to the context
                    Volatility::Stable | Volatility::Volatile => {
                        is_applicable = false;
                        VisitRecursion::Stop
                    }
                }
            }
            Expr::ScalarUDF(ScalarUDF { fun, .. }) => {
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
            | Expr::Placeholder(_) => {
                is_applicable = false;
                VisitRecursion::Stop
            }
        })
    })
    .unwrap();
    is_applicable
}

/// The maximum number of concurrent listing requests
const CONCURRENCY_LIMIT: usize = 100;

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

struct Partition {
    /// The path to the partition, including the table prefix
    path: Path,
    /// How many path segments below the table prefix `path` contains
    /// or equivalently the number of partition values in `path`
    depth: usize,
    /// The files contained as direct children of this `Partition` if known
    files: Option<Vec<ObjectMeta>>,
}

impl Partition {
    /// List the direct children of this partition updating `self.files` with
    /// any child files, and returning a list of child "directories"
    async fn list(mut self, store: &dyn ObjectStore) -> Result<(Self, Vec<Path>)> {
        trace!("Listing partition {}", self.path);
        let prefix = Some(&self.path).filter(|p| !p.as_ref().is_empty());
        let result = store.list_with_delimiter(prefix).await?;
        self.files = Some(result.objects);
        Ok((self, result.common_prefixes))
    }
}

/// Returns a recursive list of the partitions in `table_path` up to `max_depth`
async fn list_partitions(
    store: &dyn ObjectStore,
    table_path: &ListingTableUrl,
    max_depth: usize,
) -> Result<Vec<Partition>> {
    let partition = Partition {
        path: table_path.prefix().clone(),
        depth: 0,
        files: None,
    };

    let mut out = Vec::with_capacity(64);

    let mut pending = vec![];
    let mut futures = FuturesUnordered::new();
    futures.push(partition.list(store));

    while let Some((partition, paths)) = futures.next().await.transpose()? {
        // If pending contains a future it implies prior to this iteration
        // `futures.len == CONCURRENCY_LIMIT`. We can therefore add a single
        // future from `pending` to the working set
        if let Some(next) = pending.pop() {
            futures.push(next)
        }

        let depth = partition.depth;
        out.push(partition);
        for path in paths {
            let child = Partition {
                path,
                depth: depth + 1,
                files: None,
            };
            match depth < max_depth {
                true => match futures.len() < CONCURRENCY_LIMIT {
                    true => futures.push(child.list(store)),
                    false => pending.push(child.list(store)),
                },
                false => out.push(child),
            }
        }
    }
    Ok(out)
}

async fn prune_partitions(
    table_path: &ListingTableUrl,
    partitions: Vec<Partition>,
    filters: &[Expr],
    partition_cols: &[(String, DataType)],
) -> Result<Vec<Partition>> {
    if filters.is_empty() {
        return Ok(partitions);
    }

    let mut builders: Vec<_> = (0..partition_cols.len())
        .map(|_| StringBuilder::with_capacity(partitions.len(), partitions.len() * 10))
        .collect();

    for partition in &partitions {
        let cols = partition_cols.iter().map(|x| x.0.as_str());
        let parsed = parse_partitions_for_path(table_path, &partition.path, cols)
            .unwrap_or_default();

        let mut builders = builders.iter_mut();
        for (p, b) in parsed.iter().zip(&mut builders) {
            b.append_value(p);
        }
        builders.for_each(|b| b.append_null());
    }

    let arrays = partition_cols
        .iter()
        .zip(builders)
        .map(|((_, d), mut builder)| {
            let array = builder.finish();
            cast(&array, d)
        })
        .collect::<Result<_, _>>()?;

    let fields: Fields = partition_cols
        .iter()
        .map(|(n, d)| Field::new(n, d.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let df_schema = DFSchema::new_with_metadata(
        partition_cols
            .iter()
            .map(|(n, d)| DFField::new_unqualified(n, d.into(), true))
            .collect(),
        Default::default(),
    )?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // TODO: Plumb this down
    let props = ExecutionProps::new();

    // Applies `filter` to `batch` returning `None` on error
    let do_filter = |filter| -> Option<ArrayRef> {
        let expr = create_physical_expr(filter, &df_schema, &schema, &props).ok()?;
        expr.evaluate(&batch)
            .ok()?
            .into_array(partitions.len())
            .ok()
    };

    //.Compute the conjunction of the filters, ignoring errors
    let mask = filters
        .iter()
        .fold(None, |acc, filter| match (acc, do_filter(filter)) {
            (Some(a), Some(b)) => Some(and(&a, b.as_boolean()).unwrap_or(a)),
            (None, Some(r)) => Some(r.as_boolean().clone()),
            (r, None) => r,
        });

    let mask = match mask {
        Some(mask) => mask,
        None => return Ok(partitions),
    };

    // Don't retain partitions that evaluated to null
    let prepared = match mask.null_count() {
        0 => mask,
        _ => prep_null_mask_filter(&mask),
    };

    // Sanity check
    assert_eq!(prepared.len(), partitions.len());

    let filtered = partitions
        .into_iter()
        .zip(prepared.values())
        .filter_map(|(p, f)| f.then_some(p))
        .collect();

    Ok(filtered)
}

/// Discover the partitions on the given path and prune out files
/// that belong to irrelevant partitions using `filters` expressions.
/// `filters` might contain expressions that can be resolved only at the
/// file level (e.g. Parquet row group pruning).
pub async fn pruned_partition_list<'a>(
    ctx: &'a SessionState,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    // if no partition col => simply list all the files
    if partition_cols.is_empty() {
        return Ok(Box::pin(
            table_path
                .list_all_files(ctx, store, file_extension)
                .await?
                .map_ok(|object_meta| object_meta.into()),
        ));
    }

    let partitions = list_partitions(store, table_path, partition_cols.len()).await?;
    debug!("Listed {} partitions", partitions.len());

    let pruned =
        prune_partitions(table_path, partitions, filters, partition_cols).await?;

    debug!("Pruning yielded {} partitions", pruned.len());

    let stream = futures::stream::iter(pruned)
        .map(move |partition: Partition| async move {
            let cols = partition_cols.iter().map(|x| x.0.as_str());
            let parsed = parse_partitions_for_path(table_path, &partition.path, cols);

            let partition_values = parsed
                .into_iter()
                .flatten()
                .zip(partition_cols)
                .map(|(parsed, (_, datatype))| {
                    ScalarValue::try_from_string(parsed.to_string(), datatype)
                })
                .collect::<Result<Vec<_>>>()?;

            let files = match partition.files {
                Some(files) => files,
                None => {
                    trace!("Recursively listing partition {}", partition.path);
                    let s = store.list(Some(&partition.path)).await?;
                    s.try_collect().await?
                }
            };

            let files = files.into_iter().filter(move |o| {
                let extension_match = o.location.as_ref().ends_with(file_extension);
                let glob_match = table_path.contains(&o.location);
                extension_match && glob_match
            });

            let stream = futures::stream::iter(files.map(move |object_meta| {
                Ok(PartitionedFile {
                    object_meta,
                    partition_values: partition_values.clone(),
                    range: None,
                    extensions: None,
                })
            }));

            Ok::<_, DataFusionError>(stream)
        })
        .buffer_unordered(CONCURRENCY_LIMIT)
        .try_flatten()
        .boxed();
    Ok(stream)
}

/// Extract the partition values for the given `file_path` (in the given `table_path`)
/// associated to the partitions defined by `table_partition_cols`
fn parse_partitions_for_path<'a, I>(
    table_path: &ListingTableUrl,
    file_path: &'a Path,
    table_partition_cols: I,
) -> Option<Vec<&'a str>>
where
    I: IntoIterator<Item = &'a str>,
{
    let subpath = table_path.strip_prefix(file_path)?;

    let mut part_values = vec![];
    for (part, pn) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == pn => part_values.push(val),
            _ => {
                debug!(
                    "Ignoring file: file_path='{}', table_path='{}', part='{}', partition_col='{}'",
                    file_path,
                    table_path,
                    part,
                    pn,
                );
                return None;
            }
        }
    }
    Some(part_values)
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use futures::StreamExt;

    use crate::logical_expr::{case, col, lit};
    use crate::test::object_store::make_test_store_and_state;

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
        let (store, state) = make_test_store_and_state(&[
            ("tablepath/mypartition=val1/notparquetfile", 100),
            ("tablepath/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            &state,
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
        let (store, state) = make_test_store_and_state(&[
            ("tablepath/mypartition=val1/file.parquet", 100),
            ("tablepath/mypartition=val2/file.parquet", 100),
            ("tablepath/mypartition=val1/other=val3/file.parquet", 100),
        ]);
        let filter = Expr::eq(col("mypartition"), lit("val1"));
        let pruned = pruned_partition_list(
            &state,
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
        let (store, state) = make_test_store_and_state(&[
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
            &state,
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
                vec![]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/othertable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                vec![]
            )
        );
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable/").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/file.csv"),
                vec!["mypartition"]
            )
        );
        // Only hive style partitioning supported for now:
        assert_eq!(
            None,
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/v1/file.csv"),
                vec!["mypartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1", "v2"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                vec!["mypartition", "otherpartition"]
            )
        );
        assert_eq!(
            Some(vec!["v1"]),
            parse_partitions_for_path(
                &ListingTableUrl::parse("file:///bucket/mytable").unwrap(),
                &Path::from("bucket/mytable/mypartition=v1/otherpartition=v2/file.csv"),
                vec!["mypartition"]
            )
        );
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
