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
    Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion,
};
use datafusion_common::Result;

impl TreeNode for LogicalPlan {
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.inputs().into_iter().apply_until_stop(f)
    }

    fn map_children<F>(mut self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        // Apply the rewrite *in place* for each child to avoid cloning
        let result = self.rewrite_children(f)?;

        // return ourself
        Ok(result.update_data(|_| self))
    }
}
