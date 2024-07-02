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

//! This module provides common traits for visiting or rewriting tree nodes easily.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use crate::{displayable, with_new_children_if_necessary, ExecutionPlan};

use datafusion_common::tree_node::{ConcreteTreeNode, DynTreeNode};
use datafusion_common::Result;

impl DynTreeNode for dyn ExecutionPlan {
    fn arc_children(&self) -> Vec<&Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>> {
        with_new_children_if_necessary(arc_self, new_children)
    }
}

/// A node object beneficial for writing optimizer rules, encapsulating an [`ExecutionPlan`] node with a payload.
/// Since there are two ways to access child plans—directly from the plan and through child nodes—it's recommended
/// to perform mutable operations via [`Self::update_plan_from_children`].
#[derive(Debug)]
pub struct PlanContext<T: Sized> {
    /// The execution plan associated with this context.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Custom data payload of the node.
    pub data: T,
    /// Child contexts of this node.
    pub children: Vec<Self>,
}

impl<T> PlanContext<T> {
    pub fn new(plan: Arc<dyn ExecutionPlan>, data: T, children: Vec<Self>) -> Self {
        Self {
            plan,
            data,
            children,
        }
    }

    pub fn update_plan_from_children(mut self) -> Result<Self> {
        let children_plans = self.children.iter().map(|c| Arc::clone(&c.plan)).collect();
        self.plan = with_new_children_if_necessary(self.plan, children_plans)?;

        Ok(self)
    }
}

impl<T: Default> PlanContext<T> {
    pub fn new_default(plan: Arc<dyn ExecutionPlan>) -> Self {
        let children = plan
            .children()
            .into_iter()
            .cloned()
            .map(Self::new_default)
            .collect();
        Self::new(plan, Default::default(), children)
    }
}

impl<T: Display> Display for PlanContext<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let node_string = displayable(self.plan.as_ref()).one_line();
        write!(f, "Node plan: {}", node_string)?;
        write!(f, "Node data: {}", self.data)?;
        write!(f, "")
    }
}

impl<T> ConcreteTreeNode for PlanContext<T> {
    fn children(&self) -> &[Self] {
        &self.children
    }

    fn take_children(mut self) -> (Self, Vec<Self>) {
        let children = std::mem::take(&mut self.children);
        (self, children)
    }

    fn with_new_children(mut self, children: Vec<Self>) -> Result<Self> {
        self.children = children;
        self.update_plan_from_children()
    }
}
