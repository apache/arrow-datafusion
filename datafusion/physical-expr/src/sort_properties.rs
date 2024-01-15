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

use std::borrow::Cow;
use std::{ops::Neg, sync::Arc};

use arrow_schema::SortOptions;

use crate::PhysicalExpr;

use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, VisitRecursionIterator};
use datafusion_common::Result;

/// To propagate [`SortOptions`] across the [`PhysicalExpr`], it is insufficient
/// to simply use `Option<SortOptions>`: There must be a differentiation between
/// unordered columns and literal values, since literals may not break the ordering
/// when they are used as a child of some binary expression when the other child has
/// some ordering. On the other hand, unordered columns cannot maintain ordering when
/// they take part in such operations.
///
/// Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
/// sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
/// we need two different variants for literals and unordered columns as literals are
/// often more ordering-friendly under most mathematical operations.
#[derive(PartialEq, Debug, Clone, Copy, Default)]
pub enum SortProperties {
    /// Use the ordinary [`SortOptions`] struct to represent ordered data:
    Ordered(SortOptions),
    // This alternative represents unordered data:
    #[default]
    Unordered,
    // Singleton is used for single-valued literal numbers:
    Singleton,
}

impl SortProperties {
    pub fn add(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, _) => *rhs,
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn sub(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn gt_or_gteq(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                *self
            }
            _ => Self::Unordered,
        }
    }

    pub fn and_or(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            (Self::Ordered(opt), Self::Singleton)
            | (Self::Singleton, Self::Ordered(opt)) => Self::Ordered(SortOptions {
                descending: opt.descending,
                nulls_first: opt.nulls_first,
            }),
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            _ => Self::Unordered,
        }
    }
}

impl Neg for SortProperties {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            SortProperties::Ordered(SortOptions {
                descending,
                nulls_first,
            }) => SortProperties::Ordered(SortOptions {
                descending: !descending,
                nulls_first,
            }),
            SortProperties::Singleton => SortProperties::Singleton,
            SortProperties::Unordered => SortProperties::Unordered,
        }
    }
}

/// The `ExprOrdering` struct is designed to aid in the determination of ordering (represented
/// by [`SortProperties`]) for a given [`PhysicalExpr`]. When analyzing the orderings
/// of a [`PhysicalExpr`], the process begins by assigning the ordering of its leaf nodes.
/// By propagating these leaf node orderings upwards in the expression tree, the overall
/// ordering of the entire [`PhysicalExpr`] can be derived.
///
/// This struct holds the necessary state information for each expression in the [`PhysicalExpr`].
/// It encapsulates the orderings (`state`) associated with the expression (`expr`), and
/// orderings of the children expressions (`children_states`). The [`ExprOrdering`] of a parent
/// expression is determined based on the [`ExprOrdering`] states of its children expressions.
#[derive(Debug, Clone)]
pub struct ExprOrdering {
    pub expr: Arc<dyn PhysicalExpr>,
    pub state: SortProperties,
    pub children: Vec<Self>,
}

impl ExprOrdering {
    /// Creates a new [`ExprOrdering`] with [`SortProperties::Unordered`] states
    /// for `expr` and its children.
    pub fn new(expr: Arc<dyn PhysicalExpr>) -> Self {
        let children = expr.children();
        Self {
            expr,
            state: Default::default(),
            children: children.into_iter().map(Self::new).collect(),
        }
    }

    /// Get a reference to each child state.
    pub fn children_state(&self) -> Vec<SortProperties> {
        self.children.iter().map(|c| c.state).collect()
    }
}

impl TreeNode for ExprOrdering {
    fn children_nodes(&self) -> Vec<Cow<Self>> {
        self.children.iter().map(Cow::Borrowed).collect()
    }

    fn map_children<F>(mut self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        if !self.children.is_empty() {
            self.children = self
                .children
                .into_iter()
                .map(transform)
                .collect::<Result<_>>()?;
        }
        Ok(self)
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        self.children.iter_mut().for_each_till_continue(f)
    }
}
