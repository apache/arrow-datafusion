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
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion, VisitRecursionIterator};
use datafusion_common::Result;
use std::borrow::Cow;

impl TreeNode for LogicalPlan {
    fn children_nodes(&self) -> Vec<Cow<Self>> {
        self.inputs().into_iter().map(Cow::Borrowed).collect()
    }

    fn visit_inner_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        self.visit_subqueries(f)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let old_children = self.inputs();
        let new_children = old_children
            .iter()
            .map(|&c| c.clone())
            .map(transform)
            .collect::<Result<Vec<_>>>()?;

        // if any changes made, make a new child
        if old_children
            .iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != &c2)
        {
            self.with_new_exprs(self.expressions(), new_children.as_slice())
        } else {
            Ok(self)
        }
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        let old_children = self.inputs();
        let mut new_children =
            old_children.iter().map(|&c| c.clone()).collect::<Vec<_>>();
        let tnr = new_children.iter_mut().for_each_till_continue(f)?;

        // if any changes made, make a new child
        if old_children
            .iter()
            .zip(new_children.iter())
            .any(|(c1, c2)| c1 != &c2)
        {
            *self = self.with_new_exprs(self.expressions(), new_children.as_slice())?;
        }
        Ok(tnr)
    }
}
