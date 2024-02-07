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

//! This module provides common traits for visiting or rewriting tree
//! data structures easily.

use std::sync::Arc;

use crate::Result;

/// If the function returns [`VisitRecursion::Continue`], the normal execution of the
/// function continues. If it returns [`VisitRecursion::Skip`], the function returns
/// with [`VisitRecursion::Continue`] to jump next recursion step, bypassing further
/// exploration of the current step. In case of [`VisitRecursion::Stop`], the function
/// return with [`VisitRecursion::Stop`] and recursion halts.
#[macro_export]
macro_rules! handle_tree_recursion {
    ($EXPR:expr) => {
        match $EXPR {
            VisitRecursion::Continue => {}
            // If the recursion should skip, do not apply to its children, let
            // the recursion continue:
            VisitRecursion::Skip => return Ok(VisitRecursion::Continue),
            // If the recursion should stop, do not apply to its children:
            VisitRecursion::Stop => return Ok(VisitRecursion::Stop),
        }
    };
}

/// Defines a visitable and rewriteable a tree node. This trait is
/// implemented for plans ([`ExecutionPlan`] and [`LogicalPlan`]) as
/// well as expression trees ([`PhysicalExpr`], [`Expr`]) in
/// DataFusion
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
pub trait TreeNode: Sized {
    /// Applies `op` to the node and its children. `op` is applied in a preoder way,
    /// and it is controlled by [`VisitRecursion`], which means result of the `op`
    /// on the self node can cause an early return.
    ///
    /// The `op` closure can be used to collect some info from the
    /// tree node or do some checking for the tree node.
    fn apply<F: FnMut(&Self) -> Result<VisitRecursion>>(
        &self,
        op: &mut F,
    ) -> Result<VisitRecursion> {
        handle_tree_recursion!(op(self)?);
        self.apply_children(&mut |node| node.apply(op))
    }

    /// Visit the tree node using the given [TreeNodeVisitor]
    /// It performs a depth first walk of an node and its children.
    ///
    /// For an node tree such as
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(ParentNode)
    /// pre_visit(ChildNode1)
    /// post_visit(ChildNode1)
    /// pre_visit(ChildNode2)
    /// post_visit(ChildNode2)
    /// post_visit(ParentNode)
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`VisitRecursion::Stop`] is returned on a call to pre_visit, no
    /// children of that node will be visited, nor is post_visit
    /// called on that node. Details see [`TreeNodeVisitor`]
    ///
    /// If using the default [`TreeNodeVisitor::post_visit`] that does
    /// nothing, [`Self::apply`] should be preferred.
    fn visit<V: TreeNodeVisitor<N = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitRecursion> {
        handle_tree_recursion!(visitor.pre_visit(self)?);
        handle_tree_recursion!(self.apply_children(&mut |node| node.visit(visitor))?);
        visitor.post_visit(self)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given `op` to the node tree.
    /// When `op` does not apply to a given node, it is left unchanged.
    /// The default tree traversal direction is transform_up(Postorder Traversal).
    fn transform<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        self.transform_up(op)
    }

    /// Transforms the tree using `f_down` and `f_up` closures. `f_down` is applied on a
    /// node while traversing the tree top-down (pre-order, before the node's children are
    /// visited) while `f_up` is applied on a node while traversing the tree bottom-up
    /// (post-order, after the the node's children are visited).
    ///
    /// The `f_down` closure takes
    /// - a `PD` type payload from its parent
    /// and returns a tuple made of:
    /// - a possibly modified node,
    /// - a `PC` type payload to pass to `f_up`,
    /// - a `Vec<PD>` type payload to propagate down to the node's children
    ///    (one `PD` element is propagated down to each child).
    ///
    /// The `f_up` closure takes
    /// - a `PC` type payload from `f_down` and
    /// - a `Vec<PU>` type payload collected from the node's children
    /// and returns a tuple made of:
    /// - a possibly modified node,
    /// - a `PU` type payload to propagate up to the node's parent.
    fn transform_with_payload<FD, PD, PC, FU, PU>(
        self,
        f_down: &mut FD,
        payload_down: PD,
        f_up: &mut FU,
    ) -> Result<(Self, PU)>
    where
        FD: FnMut(Self, PD) -> Result<(Transformed<Self>, Vec<PD>, PC)>,
        FU: FnMut(Self, PC, Vec<PU>) -> Result<(Transformed<Self>, PU)>,
    {
        let (new_node, new_payload_down, payload_current) = f_down(self, payload_down)?;
        let mut new_payload_down_iter = new_payload_down.into_iter();
        let mut payload_up = vec![];
        let node_with_new_children = new_node.into().map_children(|node| {
            let (new_node, p) = node.transform_with_payload(
                f_down,
                new_payload_down_iter.next().unwrap(),
                f_up,
            )?;
            payload_up.push(p);
            Ok(new_node)
        })?;
        let (new_node, new_payload_up) =
            f_up(node_with_new_children, payload_current, payload_up)?;
        Ok((new_node.into(), new_payload_up))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down(op))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal) using a mutable function, `F`.
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down_mut(op))
    }

    /// Transforms the tree using `f` closure. `f` is applied on a node while traversing
    /// the tree top-down (pre-order, before the node's children are visited).
    ///
    /// The `f` closure takes
    /// - a `P` type payload from its parent
    /// and returns a tuple made of:
    /// - a possibly modified node,
    /// - a `Vec<P>` type payload to propagate down to the node's children
    ///    (one `P` element is propagated down to each child).
    fn transform_down_with_payload<F, P>(self, f: &mut F, payload: P) -> Result<Self>
    where
        F: FnMut(Self, P) -> Result<(Transformed<Self>, Vec<P>)>,
    {
        let (new_node, new_payload) = f(self, payload)?;
        let mut new_payload_iter = new_payload.into_iter();
        new_node.into().map_children(|node| {
            node.transform_down_with_payload(f, new_payload_iter.next().unwrap())
        })
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up(op))?;
        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal) using a mutable function, `F`.
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up_mut(op))?;
        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    /// Transforms the tree using `f` closure. `f` is applied on a node while traversing
    /// the tree bottom-up (post-order, after the the node's children are visited).
    ///
    /// The `f_up` closure takes
    /// - a `Vec<P>` type payload collected from the node's children
    /// and returns a tuple made of:
    /// - a possibly modified node,
    /// - a `P` type payload to propagate up to the node's parent.
    fn transform_up_with_payload<F, P>(self, f: &mut F) -> Result<(Self, P)>
    where
        F: FnMut(Self, Vec<P>) -> Result<(Transformed<Self>, P)>,
    {
        let mut payload = vec![];
        let node_with_new_children = self.map_children(|node| {
            let (new_node, p) = node.transform_up_with_payload(f)?;
            payload.push(p);
            Ok(new_node)
        })?;
        let (new_node, new_payload) = f(node_with_new_children, payload)?;
        Ok((new_node.into(), new_payload))
    }

    /// Transform the tree node using the given [TreeNodeRewriter]
    /// It performs a depth first walk of an node and its children.
    ///
    /// For an node tree such as
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(ParentNode)
    /// pre_visit(ChildNode1)
    /// mutate(ChildNode1)
    /// pre_visit(ChildNode2)
    /// mutate(ChildNode2)
    /// mutate(ParentNode)
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`false`] is returned on a call to pre_visit, no
    /// children of that node will be visited, nor is mutate
    /// called on that node
    ///
    /// If using the default [`TreeNodeRewriter::pre_visit`] which
    /// returns `true`, [`Self::transform`] should be preferred.
    fn rewrite<R: TreeNodeRewriter<N = Self>>(self, rewriter: &mut R) -> Result<Self> {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => return rewriter.mutate(self),
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };

        let after_op_children = self.map_children(|node| node.rewrite(rewriter))?;

        // now rewrite this node itself
        if need_mutate {
            rewriter.mutate(after_op_children)
        } else {
            Ok(after_op_children)
        }
    }

    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>;

    /// Apply transform `F` to the node's children, the transform `F` might have a direction(Preorder or Postorder)
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>;
}

/// Implements the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively walking [`TreeNode`]s.
///
/// [`TreeNodeVisitor`] allows keeping the algorithms
/// separate from the code to traverse the structure of the `TreeNode`
/// tree and makes it easier to add new types of tree node and
/// algorithms.
///
/// When passed to[`TreeNode::visit`], [`TreeNodeVisitor::pre_visit`]
/// and [`TreeNodeVisitor::post_visit`] are invoked recursively
/// on an node tree.
///
/// If an [`Err`] result is returned, recursion is stopped
/// immediately.
///
/// If [`VisitRecursion::Stop`] is returned on a call to pre_visit, no
/// children of that tree node are visited, nor is post_visit
/// called on that tree node
///
/// If [`VisitRecursion::Stop`] is returned on a call to post_visit, no
/// siblings of that tree node are visited, nor is post_visit
/// called on its parent tree node
///
/// If [`VisitRecursion::Skip`] is returned on a call to pre_visit, no
/// children of that tree node are visited.
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type N: TreeNode;

    /// Invoked before any children of `node` are visited.
    fn pre_visit(&mut self, node: &Self::N) -> Result<VisitRecursion>;

    /// Invoked after all children of `node` are visited. Default
    /// implementation does nothing.
    fn post_visit(&mut self, _node: &Self::N) -> Result<VisitRecursion> {
        Ok(VisitRecursion::Continue)
    }
}

/// Trait for potentially recursively transform an [`TreeNode`] node
/// tree. When passed to `TreeNode::rewrite`, `TreeNodeRewriter::mutate` is
/// invoked recursively on all nodes of a tree.
pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type N: TreeNode;

    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(Recursion::Continue)`
    fn pre_visit(&mut self, _node: &Self::N) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Self::N) -> Result<Self::N>;
}

/// Controls how the [`TreeNode`] recursion should proceed for [`TreeNode::rewrite`].
#[derive(Debug)]
pub enum RewriteRecursion {
    /// Continue rewrite this node tree.
    Continue,
    /// Call 'op' immediately and return.
    Mutate,
    /// Do not rewrite the children of this node.
    Stop,
    /// Keep recursive but skip apply op on this node
    Skip,
}

/// Controls how the [`TreeNode`] recursion should proceed for [`TreeNode::visit`].
#[derive(Debug)]
pub enum VisitRecursion {
    /// Continue the visit to this node tree.
    Continue,
    /// Keep recursive but skip applying op on the children
    Skip,
    /// Stop the visit to this node tree.
    Stop,
}

pub enum Transformed<T> {
    /// The item was transformed / rewritten somehow
    Yes(T),
    /// The item was not transformed
    No(T),
}

impl<T> Transformed<T> {
    pub fn into(self) -> T {
        match self {
            Transformed::Yes(t) => t,
            Transformed::No(t) => t,
        }
    }

    pub fn into_pair(self) -> (T, bool) {
        match self {
            Transformed::Yes(t) => (t, true),
            Transformed::No(t) => (t, false),
        }
    }
}

/// Helper trait for implementing [`TreeNode`] that have children stored as Arc's
///
/// If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`TreeNode`]
pub trait DynTreeNode {
    /// Returns all children of the specified TreeNode
    fn arc_children(&self) -> Vec<Arc<Self>>;

    /// construct a new self with the specified children
    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

/// Blanket implementation for Arc for any tye that implements
/// [`DynTreeNode`] (such as [`Arc<dyn PhysicalExpr>`])
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.arc_children() {
            handle_tree_recursion!(op(&child)?)
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children =
                children.into_iter().map(transform).collect::<Result<_>>()?;
            let arc_self = Arc::clone(&self);
            self.with_new_arc_children(arc_self, new_children)
        } else {
            Ok(self)
        }
    }
}

/// Instead of implementing [`TreeNode`], it's recommended to implement a [`ConcreteTreeNode`] for
/// trees that contain nodes with payloads. This approach ensures safe execution of algorithms
/// involving payloads, by enforcing rules for detaching and reattaching child nodes.
pub trait ConcreteTreeNode: Sized {
    /// Provides read-only access to child nodes.
    fn children(&self) -> Vec<&Self>;

    /// Detaches the node from its children, returning the node itself and its detached children.
    fn take_children(self) -> (Self, Vec<Self>);

    /// Reattaches updated child nodes to the node, returning the updated node.
    fn with_new_children(self, children: Vec<Self>) -> Result<Self>;
}

impl<T: ConcreteTreeNode> TreeNode for T {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<VisitRecursion>
    where
        F: FnMut(&Self) -> Result<VisitRecursion>,
    {
        for child in self.children() {
            handle_tree_recursion!(op(child)?)
        }
        Ok(VisitRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let (new_self, children) = self.take_children();
        if !children.is_empty() {
            let new_children =
                children.into_iter().map(transform).collect::<Result<_>>()?;
            new_self.with_new_children(new_children)
        } else {
            Ok(new_self)
        }
    }
}
