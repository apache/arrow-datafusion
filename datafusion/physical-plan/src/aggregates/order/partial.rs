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

use arrow::row::{OwnedRow, RowConverter, Rows, SortField};
use arrow_array::ArrayRef;
use arrow_schema::Schema;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use datafusion_physical_expr::PhysicalSortExpr;
use std::sync::Arc;

/// Tracks grouping state when the data is ordered by some subset of
/// the group keys.
///
/// Once the next *sort key* value is seen, never see groups with that
/// sort key again, so we can emit all groups with the previous sort
/// key and earlier.
///
/// For example, given `SUM(amt) GROUP BY id, state` if the input is
/// sorted by `state, when a new value of `state` is seen, all groups
/// with prior values of `state` can be emitted.
///
/// The state is tracked like this:
///
/// ```text
///                                            ┏━━━━━━━━━━━━━━━━━┓ ┏━━━━━━━┓
///     ┌─────┐    ┌───────────────────┐ ┌─────┃        9        ┃ ┃ "MD"  ┃
///     │┌───┐│    │ ┌──────────────┐  │ │     ┗━━━━━━━━━━━━━━━━━┛ ┗━━━━━━━┛
///     ││ 0 ││    │ │  123, "MA"   │  │ │        current_sort      sort_key
///     │└───┘│    │ └──────────────┘  │ │
///     │ ... │    │    ...            │ │      current_sort tracks the
///     │┌───┐│    │ ┌──────────────┐  │ │      smallest group index that had
///     ││ 8 ││    │ │  765, "MA"   │  │ │      the same sort_key as current
///     │├───┤│    │ ├──────────────┤  │ │
///     ││ 9 ││    │ │  923, "MD"   │◀─┼─┘
///     │├───┤│    │ ├──────────────┤  │        ┏━━━━━━━━━━━━━━┓
///     ││10 ││    │ │  345, "MD"   │  │  ┌─────┃      11      ┃
///     │├───┤│    │ ├──────────────┤  │  │     ┗━━━━━━━━━━━━━━┛
///     ││11 ││    │ │  124, "MD"   │◀─┼──┘         current
///     │└───┘│    │ └──────────────┘  │
///     └─────┘    └───────────────────┘
///
///  group indices
/// (in group value  group_values               current tracks the most
///      order)                                    recent group index
///```
#[derive(Debug)]
pub(crate) struct GroupOrderingPartial {
    /// State machine
    state: State,

    /// The indexes of the group by columns that form the sort key.
    /// For example if grouping by `id, state` and ordered by `state`
    /// this would be `[1]`.
    order_indices: Vec<usize>,

    /// Converter for the sort key (used on the group columns
    /// specified in `order_indexes`)
    row_converter: RowConverter,
}

#[derive(Debug, Default)]
enum State {
    /// The ordering was temporarily taken.  `Self::Taken` is left
    /// when state must be temporarily taken to satisfy the borrow
    /// checker. If an error happens before the state can be restored,
    /// the ordering information is lost and execution can not
    /// proceed, but there is no undefined behavior.
    #[default]
    Taken,

    /// Seen no input yet
    Start,

    /// Data is in progress.
    InProgress {
        /// Smallest group index with the sort_key
        current_sort: usize,
        /// The sort key of group_index `current_sort`
        sort_key: OwnedRow,
        /// index of the current group for which values are being
        /// generated
        current: usize,
    },

    /// Seen end of input, all groups can be emitted
    Complete,
}

impl GroupOrderingPartial {
    pub fn try_new(
        input_schema: &Schema,
        order_indices: &[usize],
        ordering: &[PhysicalSortExpr],
    ) -> Result<Self> {
        assert!(!order_indices.is_empty());
        assert!(order_indices.len() <= ordering.len());

        // get only the section of ordering, that consist of group by expressions.
        let fields = ordering[0..order_indices.len()]
            .iter()
            .map(|sort_expr| {
                Ok(SortField::new_with_options(
                    sort_expr.expr.data_type(input_schema)?,
                    sort_expr.options,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            state: State::Start,
            order_indices: order_indices.to_vec(),
            row_converter: RowConverter::new(fields)?,
        })
    }

    /// Creates sort keys from the group values
    ///
    /// For example, if group_values had `A, B, C` but the input was
    /// only sorted on `B` and `C` this should return rows for (`B`,
    /// `C`)
    fn compute_sort_keys(&mut self, group_values: &[ArrayRef]) -> Result<Rows> {
        // Take only the columns that are in the sort key
        let sort_values: Vec<_> = self
            .order_indices
            .iter()
            .map(|&idx| Arc::clone(&group_values[idx]))
            .collect();

        Ok(self.row_converter.convert_columns(&sort_values)?)
    }

    /// How many groups be emitted, or None if no data can be emitted
    pub fn emit_to(&self) -> Option<EmitTo> {
        match &self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => None,
            State::InProgress { current_sort, .. } => {
                // Can not emit if we are still on the first row sort
                // row otherwise we can emit all groups that had earlier sort keys
                //
                if *current_sort == 0 {
                    None
                } else {
                    Some(EmitTo::First(*current_sort))
                }
            }
            State::Complete => Some(EmitTo::All),
        }
    }

    /// remove the first n groups from the internal state, shifting
    /// all existing indexes down by `n`
    pub fn remove_groups(&mut self, n: usize) {
        match &mut self.state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => panic!("invalid state: start"),
            State::InProgress {
                current_sort,
                current,
                sort_key: _,
            } => {
                // shift indexes down by n
                assert!(*current >= n);
                *current -= n;
                assert!(*current_sort >= n);
                *current_sort -= n;
            }
            State::Complete { .. } => panic!("invalid state: complete"),
        }
    }

    /// Note that the input is complete so any outstanding groups are done as well
    pub fn input_done(&mut self) {
        self.state = match self.state {
            State::Taken => unreachable!("State previously taken"),
            _ => State::Complete,
        };
    }

    /// Called when new groups are added in a batch. See documentation
    /// on [`super::GroupOrdering::new_groups`]
    pub fn new_groups(
        &mut self,
        batch_group_values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        assert!(total_num_groups > 0);
        assert!(!batch_group_values.is_empty());

        let max_group_index = total_num_groups - 1;

        // compute the sort key values for each group
        let sort_keys = self.compute_sort_keys(batch_group_values)?;

        let old_state = std::mem::take(&mut self.state);
        let (mut current_sort, mut sort_key) = match &old_state {
            State::Taken => unreachable!("State previously taken"),
            State::Start => (0, sort_keys.row(0)),
            State::InProgress {
                current_sort,
                sort_key,
                ..
            } => (*current_sort, sort_key.row()),
            State::Complete => {
                panic!("Saw new group after the end of input");
            }
        };

        // Find latest sort key
        let iter = group_indices.iter().zip(sort_keys.iter());
        for (&group_index, group_sort_key) in iter {
            // Does this group have seen a new sort_key?
            if sort_key != group_sort_key {
                current_sort = group_index;
                sort_key = group_sort_key;
            }
        }

        self.state = State::InProgress {
            current_sort,
            sort_key: sort_key.owned(),
            current: max_group_index,
        };

        Ok(())
    }

    /// Return the size of memory allocated by this structure
    pub(crate) fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.order_indices.allocated_size()
            + self.row_converter.size()
    }
}
