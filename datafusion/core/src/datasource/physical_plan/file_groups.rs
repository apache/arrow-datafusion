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

//! Logic for managing groups of [`PartitionedFile`]s in DataFusion

use crate::datasource::listing::{FileRange, PartitionedFile};
use itertools::Itertools;
use std::cmp::min;
use std::collections::BinaryHeap;
use std::iter::repeat_with;

/// Repartition input files into `target_partitions` partitions, if total file size exceed
/// `repartition_file_min_size`
///
/// This partitions evenly by file byte range, and does not have any knowledge
/// of how data is laid out in specific files. The specific `FileOpener` are
/// responsible for the actual partitioning on specific data source type. (e.g.
/// the `CsvOpener` will read lines overlap with byte range as well as
/// handle boundaries to ensure all lines will be read exactly once)
///
/// # Example
///
/// For example, if there are two files `A` and `B` that we wish to read with 4
/// partitions (with 4 threads) they will be divided as follows:
///
/// ```text
///                                    ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                                      ┌─────────────────┐
///                                    │ │                 │ │
///                                      │     File A      │
///                                    │ │  Range: 0-2MB   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │   Range 2-4MB   │ │
/// │                 │                  │                 │
/// │                 │                │ └─────────────────┘ │
/// │  File A (7MB)   │   ────────▶     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │  Range: 4-6MB   │ │
/// │                 │                  │                 │
/// │                 │                │ └─────────────────┘ │
/// └─────────────────┘                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ┌─────────────────┐                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │  File B (1MB)   │                  ┌─────────────────┐
/// │                 │                │ │     File A      │ │
/// └─────────────────┘                  │  Range: 6-7MB   │
///                                    │ └─────────────────┘ │
///                                      ┌─────────────────┐
///                                    │ │  File B (1MB)   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///
///                                    If target_partitions = 4,
///                                      divides into 4 groups
/// ```
///
/// # Maintaining Order
///
/// Within each group files are read sequentially. Thus, if the overall order of
/// tuples must be preserved, multiple files can not be mixed in the same group.
///
/// In this case, the code will split the largest files evenly into any
/// available empty groups, but the overall distribution may not not be as even
/// as as even as if the order did not need to be preserved.
///
/// ```text
///                                   ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                                      ┌─────────────────┐
///                                    │ │                 │ │
///                                      │     File A      │
///                                    │ │  Range: 0-2MB   │ │
///                                      │                 │
/// ┌─────────────────┐                │ └─────────────────┘ │
/// │                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │   Range 2-4MB   │ │
/// │  File A (6MB)   │   ────────▶      │                 │
/// │    (ordered)    │                │ └─────────────────┘ │
/// │                 │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │                 │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// │                 │                  ┌─────────────────┐
/// │                 │                │ │                 │ │
/// │                 │                  │     File A      │
/// │                 │                │ │  Range: 4-6MB   │ │
/// └─────────────────┘                  │                 │
/// ┌─────────────────┐                │ └─────────────────┘ │
/// │  File B (1MB)   │                 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// │    (ordered)    │                ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
/// └─────────────────┘                  ┌─────────────────┐
///                                    │ │  File B (1MB)   │ │
///                                      │                 │
///                                    │ └─────────────────┘ │
///                                     ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///
///                                    If target_partitions = 4,
///                                      divides into 4 groups
/// ```
#[derive(Debug, Clone, Copy)]
pub struct FileGroupPartitioner {
    /// how many partitions should be created
    target_partitions: usize,
    /// the minimum size for a file to be repartitioned.
    repartition_file_min_size: usize,
    /// if the order when reading the files must be preserved
    preserve_order_within_groups: bool,
}

impl Default for FileGroupPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl FileGroupPartitioner {
    /// Creates a new [`FileGroupPartitioner`] with default values:
    /// 1. `target_partitions = 1`
    /// 2. `repartition_file_min_size = 10MB`
    /// 3. `preserve_order_within_groups = false`
    pub fn new() -> Self {
        Self {
            target_partitions: 1,
            repartition_file_min_size: 10 * 1024 * 1024,
            preserve_order_within_groups: false,
        }
    }

    /// Set the target partitions
    pub fn with_target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = target_partitions;
        self
    }

    /// Set the minimum size at which to repartition a file
    pub fn with_repartition_file_min_size(
        mut self,
        repartition_file_min_size: usize,
    ) -> Self {
        self.repartition_file_min_size = repartition_file_min_size;
        self
    }

    /// Set whether the order of tuples within a file must be preserved
    pub fn with_preserve_order_within_groups(
        mut self,
        preserve_order_within_groups: bool,
    ) -> Self {
        self.preserve_order_within_groups = preserve_order_within_groups;
        self
    }

    /// Repartition input files according to the settings on this [`FileGroupPartitioner`].
    ///
    /// If no repartitioning is needed or possible, return `None`.
    pub fn repartition_file_groups(
        &self,
        file_groups: &[Vec<PartitionedFile>],
    ) -> Option<Vec<Vec<PartitionedFile>>> {
        if file_groups.is_empty() {
            return None;
        }

        // Perform redistribution only in case all files should be read from beginning to end
        let has_ranges = file_groups.iter().flatten().any(|f| f.range.is_some());
        if has_ranges {
            return None;
        }

        //  special case when order must be preserved
        if self.preserve_order_within_groups {
            self.repartition_preserving_order(file_groups)
        } else {
            self.repartition_evenly_by_size(file_groups)
        }
    }

    /// Evenly repartition files across partitions by size, ignoring any
    /// existing grouping / ordering
    fn repartition_evenly_by_size(
        &self,
        file_groups: &[Vec<PartitionedFile>],
    ) -> Option<Vec<Vec<PartitionedFile>>> {
        let target_partitions = self.target_partitions;
        let repartition_file_min_size = self.repartition_file_min_size;
        let flattened_files = file_groups.iter().flatten().collect::<Vec<_>>();

        let total_size = flattened_files
            .iter()
            .map(|f| f.object_meta.size as i64)
            .sum::<i64>();
        if total_size < (repartition_file_min_size as i64) || total_size == 0 {
            return None;
        }

        let target_partition_size =
            (total_size as usize + (target_partitions) - 1) / (target_partitions);

        let current_partition_index: usize = 0;
        let current_partition_size: usize = 0;

        // Partition byte range evenly for all `PartitionedFile`s
        let repartitioned_files = flattened_files
            .into_iter()
            .scan(
                (current_partition_index, current_partition_size),
                |state, source_file| {
                    let mut produced_files = vec![];
                    let mut range_start = 0;
                    while range_start < source_file.object_meta.size {
                        let range_end = min(
                            range_start + (target_partition_size - state.1),
                            source_file.object_meta.size,
                        );

                        let mut produced_file = source_file.clone();
                        produced_file.range = Some(FileRange {
                            start: range_start as i64,
                            end: range_end as i64,
                        });
                        produced_files.push((state.0, produced_file));

                        if state.1 + (range_end - range_start) >= target_partition_size {
                            state.0 += 1;
                            state.1 = 0;
                        } else {
                            state.1 += range_end - range_start;
                        }
                        range_start = range_end;
                    }
                    Some(produced_files)
                },
            )
            .flatten()
            .chunk_by(|(partition_idx, _)| *partition_idx)
            .into_iter()
            .map(|(_, group)| group.map(|(_, vals)| vals).collect_vec())
            .collect_vec();

        Some(repartitioned_files)
    }

    /// Redistribute file groups across size preserving order
    fn repartition_preserving_order(
        &self,
        file_groups: &[Vec<PartitionedFile>],
    ) -> Option<Vec<Vec<PartitionedFile>>> {
        // Can't repartition and preserve order if there are more groups
        // than partitions
        if file_groups.len() >= self.target_partitions {
            return None;
        }
        let num_new_groups = self.target_partitions - file_groups.len();

        // If there is only a single file
        if file_groups.len() == 1 && file_groups[0].len() == 1 {
            return self.repartition_evenly_by_size(file_groups);
        }

        // Find which files could be split (single file groups)
        let mut heap: BinaryHeap<_> = file_groups
            .iter()
            .enumerate()
            .filter_map(|(group_index, group)| {
                // ignore groups that do not have exactly 1 file
                if group.len() == 1 {
                    Some(ToRepartition {
                        source_index: group_index,
                        file_size: group[0].object_meta.size,
                        new_groups: vec![group_index],
                    })
                } else {
                    None
                }
            })
            .collect();

        // No files can be redistributed
        if heap.is_empty() {
            return None;
        }

        // Add new empty groups to which we will redistribute ranges of existing files
        let mut file_groups: Vec<_> = file_groups
            .iter()
            .cloned()
            .chain(repeat_with(Vec::new).take(num_new_groups))
            .collect();

        // Divide up empty groups
        for (group_index, group) in file_groups.iter().enumerate() {
            if !group.is_empty() {
                continue;
            }
            // Pick the file that has the largest ranges to read so far
            let mut largest_group = heap.pop().unwrap();
            largest_group.new_groups.push(group_index);
            heap.push(largest_group);
        }

        // Distribute files to their newly assigned groups
        while let Some(to_repartition) = heap.pop() {
            let range_size = to_repartition.range_size() as i64;
            let ToRepartition {
                source_index,
                file_size,
                new_groups,
            } = to_repartition;
            assert_eq!(file_groups[source_index].len(), 1);
            let original_file = file_groups[source_index].pop().unwrap();

            let last_group = new_groups.len() - 1;
            let mut range_start: i64 = 0;
            let mut range_end: i64 = range_size;
            for (i, group_index) in new_groups.into_iter().enumerate() {
                let target_group = &mut file_groups[group_index];
                assert!(target_group.is_empty());

                // adjust last range to include the entire file
                if i == last_group {
                    range_end = file_size as i64;
                }
                target_group
                    .push(original_file.clone().with_range(range_start, range_end));
                range_start = range_end;
                range_end += range_size;
            }
        }

        Some(file_groups)
    }
}

/// Tracks how a individual file will be repartitioned
#[derive(Debug, Clone, PartialEq, Eq)]
struct ToRepartition {
    /// the index from which the original file will be taken
    source_index: usize,
    /// the size of the original file
    file_size: usize,
    /// indexes of which group(s) will this be distributed to (including `source_index`)
    new_groups: Vec<usize>,
}

impl ToRepartition {
    // how big will each file range be when this file is read in its new groups?
    fn range_size(&self) -> usize {
        self.file_size / self.new_groups.len()
    }
}

impl PartialOrd for ToRepartition {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Order based on individual range
impl Ord for ToRepartition {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.range_size().cmp(&other.range_size())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Empty file won't get partitioned
    #[test]
    fn repartition_empty_file_only() {
        let partitioned_file_empty = pfile("empty", 0);
        let file_group = vec![vec![partitioned_file_empty.clone()]];

        let partitioned_files = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(0)
            .repartition_file_groups(&file_group);

        assert_partitioned_files(None, partitioned_files);
    }

    /// Repartition when there is a empty file in file groups
    #[test]
    fn repartition_empty_files() {
        let pfile_a = pfile("a", 10);
        let pfile_b = pfile("b", 10);
        let pfile_empty = pfile("empty", 0);

        let empty_first = vec![
            vec![pfile_empty.clone()],
            vec![pfile_a.clone()],
            vec![pfile_b.clone()],
        ];
        let empty_middle = vec![
            vec![pfile_a.clone()],
            vec![pfile_empty.clone()],
            vec![pfile_b.clone()],
        ];
        let empty_last = vec![vec![pfile_a], vec![pfile_b], vec![pfile_empty]];

        // Repartition file groups into x partitions
        let expected_2 = vec![
            vec![pfile("a", 10).with_range(0, 10)],
            vec![pfile("b", 10).with_range(0, 10)],
        ];
        let expected_3 = vec![
            vec![pfile("a", 10).with_range(0, 7)],
            vec![
                pfile("a", 10).with_range(7, 10),
                pfile("b", 10).with_range(0, 4),
            ],
            vec![pfile("b", 10).with_range(4, 10)],
        ];

        let file_groups_tests = [empty_first, empty_middle, empty_last];

        for fg in file_groups_tests {
            let all_expected = [(2, expected_2.clone()), (3, expected_3.clone())];
            for (n_partition, expected) in all_expected {
                let actual = FileGroupPartitioner::new()
                    .with_target_partitions(n_partition)
                    .with_repartition_file_min_size(10)
                    .repartition_file_groups(&fg);

                assert_partitioned_files(Some(expected), actual);
            }
        }
    }

    #[test]
    fn repartition_single_file() {
        // Single file, single partition into multiple partitions
        let single_partition = vec![vec![pfile("a", 123)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            vec![pfile("a", 123).with_range(0, 31)],
            vec![pfile("a", 123).with_range(31, 62)],
            vec![pfile("a", 123).with_range(62, 93)],
            vec![pfile("a", 123).with_range(93, 123)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_too_much_partitions() {
        // Single file, single partition into 96 partitions
        let partitioned_file = pfile("a", 8);
        let single_partition = vec![vec![partitioned_file]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(96)
            .with_repartition_file_min_size(5)
            .repartition_file_groups(&single_partition);

        let expected = Some(vec![
            vec![pfile("a", 8).with_range(0, 1)],
            vec![pfile("a", 8).with_range(1, 2)],
            vec![pfile("a", 8).with_range(2, 3)],
            vec![pfile("a", 8).with_range(3, 4)],
            vec![pfile("a", 8).with_range(4, 5)],
            vec![pfile("a", 8).with_range(5, 6)],
            vec![pfile("a", 8).with_range(6, 7)],
            vec![pfile("a", 8).with_range(7, 8)],
        ]);

        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_multiple_partitions() {
        // Multiple files in single partition after redistribution
        let source_partitions = vec![vec![pfile("a", 40)], vec![pfile("b", 60)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            vec![pfile("a", 40).with_range(0, 34)],
            vec![
                pfile("a", 40).with_range(34, 40),
                pfile("b", 60).with_range(0, 28),
            ],
            vec![pfile("b", 60).with_range(28, 60)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_same_num_partitions() {
        // "Rebalance" files across partitions
        let source_partitions = vec![vec![pfile("a", 40)], vec![pfile("b", 60)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(2)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            vec![
                pfile("a", 40).with_range(0, 40),
                pfile("b", 60).with_range(0, 10),
            ],
            vec![pfile("b", 60).with_range(10, 60)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_no_action_ranges() {
        // No action due to Some(range) in second file
        let source_partitions = vec![
            vec![pfile("a", 123)],
            vec![pfile("b", 144).with_range(1, 50)],
        ];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_no_action_min_size() {
        // No action due to target_partition_size
        let single_partition = vec![vec![pfile("a", 123)]];

        let actual = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(500)
            .repartition_file_groups(&single_partition);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_no_action_zero_files() {
        // No action due to no files
        let empty_partition = vec![];

        let partitioner = FileGroupPartitioner::new()
            .with_target_partitions(65)
            .with_repartition_file_min_size(500);

        assert_partitioned_files(None, repartition_test(partitioner, empty_partition))
    }

    #[test]
    fn repartition_ordered_no_action_too_few_partitions() {
        // No action as there are no new groups to redistribute to
        let input_partitions = vec![vec![pfile("a", 100)], vec![pfile("b", 200)]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(2)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&input_partitions);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_ordered_no_action_file_too_small() {
        // No action as there are no new groups to redistribute to
        let single_partition = vec![vec![pfile("a", 100)]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(2)
            // file is too small to repartition
            .with_repartition_file_min_size(1000)
            .repartition_file_groups(&single_partition);

        assert_partitioned_files(None, actual)
    }

    #[test]
    fn repartition_ordered_one_large_file() {
        // "Rebalance" the single large file across partitions
        let source_partitions = vec![vec![pfile("a", 100)]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            vec![pfile("a", 100).with_range(0, 34)],
            vec![pfile("a", 100).with_range(34, 68)],
            vec![pfile("a", 100).with_range(68, 100)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_file() {
        // "Rebalance" the single large file across empty partitions, but can't split
        // small file
        let source_partitions = vec![vec![pfile("a", 100)], vec![pfile("b", 30)]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first third of "a"
            vec![pfile("a", 100).with_range(0, 33)],
            // only b in this group (can't do this)
            vec![pfile("b", 30).with_range(0, 30)],
            // second third of "a"
            vec![pfile("a", 100).with_range(33, 66)],
            // final third of "a"
            vec![pfile("a", 100).with_range(66, 100)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_two_large_files() {
        // "Rebalance" two large files across empty partitions, but can't mix them
        let source_partitions = vec![vec![pfile("a", 100)], vec![pfile("b", 100)]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(4)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            vec![pfile("a", 100).with_range(0, 50)],
            // scan first half of "b"
            vec![pfile("b", 100).with_range(0, 50)],
            // second half of "a"
            vec![pfile("a", 100).with_range(50, 100)],
            // second half of "b"
            vec![pfile("b", 100).with_range(50, 100)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_two_large_one_small_files() {
        // "Rebalance" two large files and one small file across empty partitions
        let source_partitions = vec![
            vec![pfile("a", 100)],
            vec![pfile("b", 100)],
            vec![pfile("c", 30)],
        ];

        let partitioner = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_repartition_file_min_size(10);

        // with 4 partitions, can only split the first large file "a"
        let actual = partitioner
            .with_target_partitions(4)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            vec![pfile("a", 100).with_range(0, 50)],
            // All of "b"
            vec![pfile("b", 100).with_range(0, 100)],
            // All of "c"
            vec![pfile("c", 30).with_range(0, 30)],
            // second half of "a"
            vec![pfile("a", 100).with_range(50, 100)],
        ]);
        assert_partitioned_files(expected, actual);

        // With 5 partitions, we can split both "a" and "b", but they can't be intermixed
        let actual = partitioner
            .with_target_partitions(5)
            .repartition_file_groups(&source_partitions);

        let expected = Some(vec![
            // scan first half of "a"
            vec![pfile("a", 100).with_range(0, 50)],
            // scan first half of "b"
            vec![pfile("b", 100).with_range(0, 50)],
            // All of "c"
            vec![pfile("c", 30).with_range(0, 30)],
            // second half of "a"
            vec![pfile("a", 100).with_range(50, 100)],
            // second half of "b"
            vec![pfile("b", 100).with_range(50, 100)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    #[test]
    fn repartition_ordered_one_large_one_small_existing_empty() {
        // "Rebalance" files using existing empty partition
        let source_partitions =
            vec![vec![pfile("a", 100)], vec![], vec![pfile("b", 40)], vec![]];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(5)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        // Of the three available groups (2 original empty and 1 new from the
        // target partitions), assign two to "a" and one to "b"
        let expected = Some(vec![
            // Scan of "a" across three groups
            vec![pfile("a", 100).with_range(0, 33)],
            vec![pfile("a", 100).with_range(33, 66)],
            // scan first half of "b"
            vec![pfile("b", 40).with_range(0, 20)],
            // final third of "a"
            vec![pfile("a", 100).with_range(66, 100)],
            // second half of "b"
            vec![pfile("b", 40).with_range(20, 40)],
        ]);
        assert_partitioned_files(expected, actual);
    }
    #[test]
    fn repartition_ordered_existing_group_multiple_files() {
        // groups with multiple files in a group can not be changed, but can divide others
        let source_partitions = vec![
            // two files in an existing partition
            vec![pfile("a", 100), pfile("b", 100)],
            vec![pfile("c", 40)],
        ];

        let actual = FileGroupPartitioner::new()
            .with_preserve_order_within_groups(true)
            .with_target_partitions(3)
            .with_repartition_file_min_size(10)
            .repartition_file_groups(&source_partitions);

        // Of the three available groups (2 original empty and 1 new from the
        // target partitions), assign two to "a" and one to "b"
        let expected = Some(vec![
            // don't try and rearrange files in the existing partition
            // assuming that the caller had a good reason to put them that way.
            // (it is technically possible to split off ranges from the files if desired)
            vec![pfile("a", 100), pfile("b", 100)],
            // first half of "c"
            vec![pfile("c", 40).with_range(0, 20)],
            // second half of "c"
            vec![pfile("c", 40).with_range(20, 40)],
        ]);
        assert_partitioned_files(expected, actual);
    }

    /// Asserts that the two groups of `ParititonedFile` are the same
    /// (PartitionedFile doesn't implement PartialEq)
    fn assert_partitioned_files(
        expected: Option<Vec<Vec<PartitionedFile>>>,
        actual: Option<Vec<Vec<PartitionedFile>>>,
    ) {
        match (expected, actual) {
            (None, None) => {}
            (Some(_), None) => panic!("Expected Some, got None"),
            (None, Some(_)) => panic!("Expected None, got Some"),
            (Some(expected), Some(actual)) => {
                let expected_string = format!("{:#?}", expected);
                let actual_string = format!("{:#?}", actual);
                assert_eq!(expected_string, actual_string);
            }
        }
    }

    /// returns a partitioned file with the specified path and size
    fn pfile(path: impl Into<String>, file_size: u64) -> PartitionedFile {
        PartitionedFile::new(path, file_size)
    }

    /// repartition the file groups both with and without preserving order
    /// asserting they return the same value and returns that value
    fn repartition_test(
        partitioner: FileGroupPartitioner,
        file_groups: Vec<Vec<PartitionedFile>>,
    ) -> Option<Vec<Vec<PartitionedFile>>> {
        let repartitioned = partitioner.repartition_file_groups(&file_groups);

        let repartitioned_preserving_sort = partitioner
            .with_preserve_order_within_groups(true)
            .repartition_file_groups(&file_groups);

        assert_partitioned_files(
            repartitioned.clone(),
            repartitioned_preserving_sort.clone(),
        );
        repartitioned
    }
}
