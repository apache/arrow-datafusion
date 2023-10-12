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

//! Defines common code used in execution plans

use super::SendableRecordBatchStream;
use crate::stream::ReceiverStream;
use crate::{ColumnStatistics, ExecutionPlan, Statistics};
use arrow::datatypes::Schema;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use datafusion_common::{plan_err, DataFusionError, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use futures::{Future, StreamExt, TryStreamExt};
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use std::fs;
use std::fs::{metadata, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::task::JoinHandle;

/// [`MemoryReservation`] used across query execution streams
pub(crate) type SharedMemoryReservation = Arc<Mutex<MemoryReservation>>;

/// Create a vector of record batches from a stream
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_checked_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    if filenames.is_empty() {
        return plan_err!("No files found at {dir} with file extension {ext}");
    }
    Ok(filenames)
}

/// Recursively builds a list of files in a directory with a given extension
pub fn build_file_list(dir: &str, ext: &str) -> Result<Vec<String>> {
    let mut filenames: Vec<String> = Vec::new();
    build_file_list_recurse(dir, &mut filenames, ext)?;
    Ok(filenames)
}

/// Recursively build a list of files in a directory with a given extension with an accumulator list
fn build_file_list_recurse(
    dir: &str,
    filenames: &mut Vec<String>,
    ext: &str,
) -> Result<()> {
    let metadata = metadata(dir)?;
    if metadata.is_file() {
        if dir.ends_with(ext) {
            filenames.push(dir.to_string());
        }
    } else {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(path_name) = path.to_str() {
                if path.is_dir() {
                    build_file_list_recurse(path_name, filenames, ext)?;
                } else if path_name.ends_with(ext) {
                    filenames.push(path_name.to_string());
                }
            } else {
                return plan_err!("Invalid path");
            }
        }
    }
    Ok(())
}

/// If running in a tokio context spawns the execution of `stream` to a separate task
/// allowing it to execute in parallel with an intermediate buffer of size `buffer`
pub(crate) fn spawn_buffered(
    mut input: SendableRecordBatchStream,
    buffer: usize,
) -> SendableRecordBatchStream {
    // Use tokio only if running from a multi-thread tokio context
    match tokio::runtime::Handle::try_current() {
        Ok(handle)
            if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread =>
        {
            let mut builder = ReceiverStream::builder(input.schema(), buffer);

            let sender = builder.tx();

            builder.spawn(async move {
                while let Some(item) = input.next().await {
                    if sender.send(item).await.is_err() {
                        // receiver dropped when query is shutdown early (e.g., limit) or error,
                        // no need to return propagate the send error.
                        return Ok(());
                    }
                }

                Ok(())
            });

            builder.build()
        }
        _ => input,
    }
}

/// Computes the statistics for an in-memory RecordBatch
///
/// Only computes statistics that are in arrows metadata (num rows, byte size and nulls)
/// and does not apply any kernel on the actual data.
pub fn compute_record_batch_statistics(
    batches: &[Vec<RecordBatch>],
    schema: &Schema,
    projection: Option<Vec<usize>>,
) -> Statistics {
    let nb_rows = batches.iter().flatten().map(RecordBatch::num_rows).sum();

    let total_byte_size = batches
        .iter()
        .flatten()
        .map(|b| b.get_array_memory_size())
        .sum();

    let projection = match projection {
        Some(p) => p,
        None => (0..schema.fields().len()).collect(),
    };

    let mut column_statistics = vec![ColumnStatistics::default(); projection.len()];

    for partition in batches.iter() {
        for batch in partition {
            for (stat_index, col_index) in projection.iter().enumerate() {
                *column_statistics[stat_index].null_count.get_or_insert(0) +=
                    batch.column(*col_index).null_count();
            }
        }
    }

    Statistics {
        num_rows: Some(nb_rows),
        total_byte_size: Some(total_byte_size),
        column_statistics: Some(column_statistics),
        is_exact: true,
    }
}

pin_project! {
    /// Helper that aborts the given join handle on drop.
    ///
    /// Useful to kill background tasks when the consumer is dropped.
    #[derive(Debug)]
    pub struct AbortOnDropSingle<T>{
        #[pin]
        join_handle: JoinHandle<T>,
    }

    impl<T> PinnedDrop for AbortOnDropSingle<T> {
        fn drop(this: Pin<&mut Self>) {
            this.join_handle.abort();
        }
    }
}

impl<T> AbortOnDropSingle<T> {
    /// Create new abort helper from join handle.
    pub fn new(join_handle: JoinHandle<T>) -> Self {
        Self { join_handle }
    }
}

impl<T> Future for AbortOnDropSingle<T> {
    type Output = Result<T, tokio::task::JoinError>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.join_handle.poll(cx)
    }
}

/// Helper that aborts the given join handles on drop.
///
/// Useful to kill background tasks when the consumer is dropped.
#[derive(Debug)]
pub struct AbortOnDropMany<T>(pub Vec<JoinHandle<T>>);

impl<T> Drop for AbortOnDropMany<T> {
    fn drop(&mut self) {
        for join_handle in &self.0 {
            join_handle.abort();
        }
    }
}

/// Transposes the given vector of vectors.
pub fn transpose<T>(original: Vec<Vec<T>>) -> Vec<Vec<T>> {
    match original.as_slice() {
        [] => vec![],
        [first, ..] => {
            let mut result = (0..first.len()).map(|_| vec![]).collect::<Vec<_>>();
            for row in original {
                for (item, transposed_row) in row.into_iter().zip(&mut result) {
                    transposed_row.push(item);
                }
            }
            result
        }
    }
}

/// Calculates the "meet" of given orderings.
/// The meet is the finest ordering that satisfied by all the given
/// orderings, see <https://en.wikipedia.org/wiki/Join_and_meet>.
pub fn get_meet_of_orderings(
    given: &[Arc<dyn ExecutionPlan>],
) -> Option<&[PhysicalSortExpr]> {
    given
        .iter()
        .map(|item| item.output_ordering())
        .collect::<Option<Vec<_>>>()
        .and_then(get_meet_of_orderings_helper)
}

fn get_meet_of_orderings_helper(
    orderings: Vec<&[PhysicalSortExpr]>,
) -> Option<&[PhysicalSortExpr]> {
    let mut idx = 0;
    let first = orderings[0];
    loop {
        for ordering in orderings.iter() {
            if idx >= ordering.len() {
                return Some(ordering);
            } else {
                let schema_aligned = check_expr_alignment(
                    ordering[idx].expr.as_ref(),
                    first[idx].expr.as_ref(),
                );
                if !schema_aligned || (ordering[idx].options != first[idx].options) {
                    // In a union, the output schema is that of the first child (by convention).
                    // Therefore, generate the result from the first child's schema:
                    return if idx > 0 { Some(&first[..idx]) } else { None };
                }
            }
        }
        idx += 1;
    }

    fn check_expr_alignment(first: &dyn PhysicalExpr, second: &dyn PhysicalExpr) -> bool {
        match (
            first.as_any().downcast_ref::<Column>(),
            second.as_any().downcast_ref::<Column>(),
            first.as_any().downcast_ref::<BinaryExpr>(),
            second.as_any().downcast_ref::<BinaryExpr>(),
        ) {
            (Some(first_col), Some(second_col), _, _) => {
                first_col.index() == second_col.index()
            }
            (_, _, Some(first_binary), Some(second_binary)) => {
                if first_binary.op() == second_binary.op() {
                    check_expr_alignment(
                        first_binary.left().as_ref(),
                        second_binary.left().as_ref(),
                    ) && check_expr_alignment(
                        first_binary.right().as_ref(),
                        second_binary.right().as_ref(),
                    )
                } else {
                    false
                }
            }
            (_, _, _, _) => false,
        }
    }
}

/// Write in Arrow IPC format.
pub struct IPCWriter {
    /// path
    pub path: PathBuf,
    /// inner writer
    pub writer: FileWriter<File>,
    /// batches written
    pub num_batches: u64,
    /// rows written
    pub num_rows: u64,
    /// bytes written
    pub num_bytes: u64,
}

impl IPCWriter {
    /// Create new writer
    pub fn new(path: &Path, schema: &Schema) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create partition file at {path:?}: {e:?}"
            ))
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new(file, schema)?,
        })
    }

    /// Create new writer with IPC write options
    pub fn new_with_options(
        path: &Path,
        schema: &Schema,
        write_options: IpcWriteOptions,
    ) -> Result<Self> {
        let file = File::create(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create partition file at {path:?}: {e:?}"
            ))
        })?;
        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            path: path.into(),
            writer: FileWriter::try_new_with_options(file, schema, write_options)?,
        })
    }
    /// Write one single batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        self.num_batches += 1;
        self.num_rows += batch.num_rows() as u64;
        let num_bytes: usize = batch.get_array_memory_size();
        self.num_bytes += num_bytes as u64;
        Ok(())
    }

    /// Finish the writer
    pub fn finish(&mut self) -> Result<()> {
        self.writer.finish().map_err(Into::into)
    }

    /// Path write to
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Returns the total number of bytes of memory occupied physically by this batch.
#[deprecated(since = "28.0.0", note = "RecordBatch::get_array_memory_size")]
pub fn batch_byte_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

#[cfg(test)]
mod tests {
    use std::ops::Not;

    use super::*;
    use crate::memory::MemoryExec;
    use crate::sorts::sort::SortExec;
    use crate::union::UnionExec;
    use arrow::compute::SortOptions;
    use arrow::{
        array::{Float32Array, Float64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{col, Column};

    #[test]
    fn get_meet_of_orderings_helper_common_prefix_test() -> Result<()> {
        let input1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];

        let input2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("x", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("y", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("z", 2)),
                options: SortOptions::default(),
            },
        ];

        let input3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("d", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("e", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("f", 2)),
                options: SortOptions::default(),
            },
        ];

        let input4: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("g", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("h", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                // Note that index of this column is not 2. Hence this 3rd entry shouldn't be
                // in the output ordering.
                expr: Arc::new(Column::new("i", 3)),
                options: SortOptions::default(),
            },
        ];

        let expected = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];
        let result = get_meet_of_orderings_helper(vec![&input1, &input2, &input3]);
        assert_eq!(result.unwrap(), expected);

        let expected = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        let result = get_meet_of_orderings_helper(vec![&input1, &input2, &input4]);
        assert_eq!(result.unwrap(), expected);
        Ok(())
    }

    #[test]
    fn get_meet_of_orderings_helper_subset_test() -> Result<()> {
        let input1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];

        let input2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("d", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("e", 2)),
                options: SortOptions::default(),
            },
        ];

        let input3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("f", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("g", 1)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("h", 2)),
                options: SortOptions::default(),
            },
        ];

        let result = get_meet_of_orderings_helper(vec![&input1, &input2, &input3]);
        assert_eq!(result.unwrap(), input1);
        Ok(())
    }

    #[test]
    fn get_meet_of_orderings_helper_no_overlap_test() -> Result<()> {
        let input1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                // Since ordering is conflicting with other inputs
                // output ordering should be empty
                options: SortOptions::default().not(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];

        let input2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("x", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 1)),
                options: SortOptions::default(),
            },
        ];

        let input3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 2)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("y", 1)),
                options: SortOptions::default(),
            },
        ];

        let result = get_meet_of_orderings_helper(vec![&input1, &input2]);
        assert!(result.is_none());

        let result = get_meet_of_orderings_helper(vec![&input2, &input3]);
        assert!(result.is_none());

        let result = get_meet_of_orderings_helper(vec![&input1, &input3]);
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn get_meet_of_orderings_helper_binary_exprs() -> Result<()> {
        let input1: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("b", 1)),
                )),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];

        let input2: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("x", 0)),
                    Operator::Plus,
                    Arc::new(Column::new("y", 1)),
                )),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("z", 2)),
                options: SortOptions::default(),
            },
        ];

        // erroneous input
        let input3: Vec<PhysicalSortExpr> = vec![
            PhysicalSortExpr {
                expr: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 1)),
                    Operator::Plus,
                    Arc::new(Column::new("b", 0)),
                )),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: SortOptions::default(),
            },
        ];

        let result = get_meet_of_orderings_helper(vec![&input1, &input2]);
        assert_eq!(input1, result.unwrap());

        let result = get_meet_of_orderings_helper(vec![&input2, &input3]);
        assert!(result.is_none());

        let result = get_meet_of_orderings_helper(vec![&input1, &input3]);
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_meet_of_orderings() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let sort_expr = vec![PhysicalSortExpr {
            expr: col("f32", &schema).unwrap(),
            options: SortOptions::default(),
        }];
        let memory_exec = Arc::new(MemoryExec::try_new(&[], schema.clone(), None)?) as _;
        let sort_exec = Arc::new(SortExec::new(sort_expr.clone(), memory_exec))
            as Arc<dyn ExecutionPlan>;
        let memory_exec2 = Arc::new(MemoryExec::try_new(&[], schema, None)?) as _;
        // memory_exec2 doesn't have output ordering
        let union_exec = UnionExec::new(vec![sort_exec.clone(), memory_exec2]);
        let res = get_meet_of_orderings(union_exec.inputs());
        assert!(res.is_none());

        let union_exec = UnionExec::new(vec![sort_exec.clone(), sort_exec]);
        let res = get_meet_of_orderings(union_exec.inputs());
        assert_eq!(res, Some(&sort_expr[..]));
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics_empty() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let stats = compute_record_batch_statistics(&[], &schema, Some(vec![0, 1]));

        assert_eq!(stats.num_rows, Some(0));
        assert!(stats.is_exact);
        assert_eq!(stats.total_byte_size, Some(0));
        Ok(())
    }

    #[test]
    fn test_compute_record_batch_statistics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f32", DataType::Float32, false),
            Field::new("f64", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float32Array::from(vec![1., 2., 3.])),
                Arc::new(Float64Array::from(vec![9., 8., 7.])),
            ],
        )?;
        let actual =
            compute_record_batch_statistics(&[vec![batch]], &schema, Some(vec![0, 1]));

        let mut expected = Statistics {
            is_exact: true,
            num_rows: Some(3),
            total_byte_size: Some(464), // this might change a bit if the way we compute the size changes
            column_statistics: Some(vec![
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: Some(0),
                },
                ColumnStatistics {
                    distinct_count: None,
                    max_value: None,
                    min_value: None,
                    null_count: Some(0),
                },
            ]),
        };

        // Prevent test flakiness due to undefined / changing implementation details
        expected.total_byte_size = actual.total_byte_size;

        assert_eq!(actual, expected);
        Ok(())
    }

    #[test]
    fn test_transpose() -> Result<()> {
        let in_data = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let transposed = transpose(in_data);
        let expected = vec![vec![1, 4], vec![2, 5], vec![3, 6]];
        assert_eq!(expected, transposed);
        Ok(())
    }
}
