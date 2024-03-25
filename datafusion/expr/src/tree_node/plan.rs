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

//! Tree node implementation for logical plan

use crate::LogicalPlan;

use datafusion_common::tree_node::{
    Transformed, TransformedIterator, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion_common::{handle_visit_recursion, Result};

impl TreeNode for LogicalPlan {
    fn apply<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        // Compared to the default implementation, we need to invoke
        // [`Self::apply_subqueries`] before visiting its children
        handle_visit_recursion!(f(self)?, DOWN);
        self.apply_subqueries(f)?;
        self.apply_children(&mut |n| n.apply(f))
    }

    /// To use, define a struct that implements the trait [`TreeNodeVisitor`] and then invoke
    /// [`LogicalPlan::visit`].
    ///
    /// For example, for a logical plan like:
    ///
    /// ```text
    /// Projection: id
    ///    Filter: state Eq Utf8(\"CO\")\
    ///       CsvScan: employee.csv projection=Some([0, 3])";
    /// ```
    ///
    /// The sequence of visit operations would be:
    /// ```text
    /// visitor.pre_visit(Projection)
    /// visitor.pre_visit(Filter)
    /// visitor.pre_visit(CsvScan)
    /// visitor.post_visit(CsvScan)
    /// visitor.post_visit(Filter)
    /// visitor.post_visit(Projection)
    /// ```
    fn visit<V: TreeNodeVisitor<Node = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        // Compared to the default implementation, we need to invoke
        // [`Self::visit_subqueries`] before visiting its children
        match visitor.f_down(self)? {
            TreeNodeRecursion::Continue => {
                self.visit_subqueries(visitor)?;
                handle_visit_recursion!(
                    self.apply_children(&mut |n| n.visit(visitor))?,
                    UP
                );
                visitor.f_up(self)
            }
            TreeNodeRecursion::Jump => {
                self.visit_subqueries(visitor)?;
                visitor.f_up(self)
            }
            TreeNodeRecursion::Stop => Ok(TreeNodeRecursion::Stop),
        }
    }

    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for child in self.inputs() {
            tnr = f(child)?;
            handle_visit_recursion!(tnr, DOWN)
        }
        Ok(tnr)
    }

    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let new_children = self
            .inputs()
            .iter()
            .map(|&c| c.clone())
            .map_until_stop_and_collect(f)?;
        // Propagate up `new_children.transformed` and `new_children.tnr`
        // along with the node containing transformed children.
        if new_children.transformed {
            new_children.map_data(|new_children| {
                self.with_new_exprs(self.expressions(), new_children)
            })
        } else {
            Ok(new_children.update_data(|_| self))
        }
    }
}
