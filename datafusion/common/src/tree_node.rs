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

use std::borrow::Cow;
use std::sync::Arc;

use crate::Result;

/// Defines a tree node that can have children of the same type as the parent node. The
/// implementations must provide [`TreeNode::visit_children()`] and
/// [`TreeNode::transform_children()`] for visiting and changing the structure of the tree.
///
/// [`TreeNode`] is implemented for plans ([`ExecutionPlan`] and [`LogicalPlan`]) as well
/// as expression trees ([`PhysicalExpr`], [`Expr`]) in DataFusion.
///
/// Besides the children, each tree node can define links to embedded trees of the same
/// type. The root node of these trees are called inner children of a node.
///
/// A logical plan of a query is a tree of [`LogicalPlan`] nodes, where each node can
/// contain multiple expression ([`Expr`]) trees. But expression tree nodes can contain
/// logical plans of subqueries, which are again trees of [`LogicalPlan`] nodes. The root
/// nodes of these subquery plans are the inner children of the containing query plan
/// node.
///
/// Tree node implementations can provide [`TreeNode::visit_inner_children()`] for
/// visiting the structure of the inner tree.
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
pub trait TreeNode: Sized + Clone {
    /// Returns all children of the TreeNode
    fn children_nodes(&self) -> Vec<Cow<Self>>;

    /// Applies `f` to the tree node, then to its inner children and then to its children
    /// depending on the result of `f` in a preorder traversal.
    /// See [`TreeNodeRecursion`] for more details on how the preorder traversal can be
    /// controlled.
    /// If an [`Err`] result is returned, recursion is stopped immediately.
    fn visit_down<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        // Apply `f` on self.
        f(self)?
            // If it returns continue (not prune or stop or stop all) then continue
            // traversal on inner children and children.
            .and_then_on_continue(|| {
                // Run the recursive `apply` on each inner children, but as they are
                // unrelated root nodes of inner trees if any returns stop then continue
                // with the next one.
                self.visit_inner_children(&mut |c| c.visit_down(f)?.continue_on_stop())?
                    // Run the recursive `apply` on each children.
                    .and_then_on_continue(|| {
                        self.visit_children(&mut |c| c.visit_down(f))
                    })
            })?
            // Applying `f` on self might have returned prune, but we need to propagate
            // continue.
            .continue_on_prune()
    }

    /// Uses a [`TreeNodeVisitor`] to visit the tree node, then its inner children and
    /// then its children depending on the result of [`TreeNodeVisitor::pre_visit()`] and
    /// [`TreeNodeVisitor::post_visit()`] in a traversal.
    /// See [`TreeNodeRecursion`] for more details on how the traversal can be controlled.
    ///
    /// If an [`Err`] result is returned, recursion is stopped immediately.
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
    /// If using the default [`TreeNodeVisitor::post_visit()`] that does  nothing,
    /// [`Self::visit_down()`] should be preferred.
    fn visit<V: TreeNodeVisitor<Node = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        // Apply `pre_visit` on self.
        visitor
            .pre_visit(self)?
            // If it returns continue (not prune or stop or stop all) then continue
            // traversal on inner children and children.
            .and_then_on_continue(|| {
                // Run the recursive `visit` on each inner children, but as they are
                // unrelated subquery plans if any returns stop then continue with the
                // next one.
                self.visit_inner_children(&mut |c| c.visit(visitor)?.continue_on_stop())?
                    // Run the recursive `visit` on each children.
                    .and_then_on_continue(|| {
                        self.visit_children(&mut |c| c.visit(visitor))
                    })?
                    // Apply `post_visit` on self.
                    .and_then_on_continue(|| visitor.post_visit(self))
            })?
            // Applying `pre_visit` or `post_visit` on self might have returned prune,
            // but we need to propagate continue.
            .continue_on_prune()
    }

    fn transform<T: TreeNodeTransformer<Node = Self>>(
        &mut self,
        transformer: &mut T,
    ) -> Result<TreeNodeRecursion> {
        // Apply `pre_transform` on self.
        transformer
            .pre_transform(self)?
            // If it returns continue (not prune or stop or stop all) then continue
            // traversal on inner children and children.
            .and_then_on_continue(||
                // Run the recursive `transform` on each children.
                self
                    .transform_children(&mut |c| c.transform(transformer))?
                    // Apply `post_transform` on new self.
                    .and_then_on_continue(|| transformer.post_transform(self)))?
            // Applying `pre_transform` or `post_transform` on self might have returned
            // prune, but we need to propagate continue.
            .continue_on_prune()
    }

    fn transform_down<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        // Apply `f` on self.
        f(self)?
            // If it returns continue (not prune or stop or stop all) then continue
            // traversal on inner children and children.
            .and_then_on_continue(||
                // Run the recursive `transform` on each children.
                self.transform_children(&mut |c| c.transform_down(f)))?
            // Applying `f` on self might have returned prune, but we need to propagate
            // continue.
            .continue_on_prune()
    }

    fn transform_up<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        // Run the recursive `transform` on each children.
        self.transform_children(&mut |c| c.transform_up(f))?
            // Apply `f` on self.
            .and_then_on_continue(|| f(self))?
            // Applying `f` on self might have returned prune, but we need to propagate
            // continue.
            .continue_on_prune()
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down_old<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down_old(op))
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

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up_old<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up_old(op))?;

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

    fn visit_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        self.children_nodes()
            .iter()
            .for_each_till_continue(&mut |c| f(c))
    }

    /// Apply `f` to the node's inner children.
    fn visit_inner_children<F>(&self, _f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Apply transform `F` to the node's children, the transform `F` might have a direction(Preorder or Postorder)
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>;

    /// Apply `f` to the node's children.
    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>;

    /// Convenience function to do a preorder traversal of the tree nodes with `f` that
    /// can't fail.
    fn for_each<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        self.visit_down(&mut |n| {
            f(n);
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
    }

    /// Convenience function to collect the first non-empty value that `f` returns in a
    /// preorder traversal.
    fn collect_first<F, R>(&self, f: &mut F) -> Option<R>
    where
        F: FnMut(&Self) -> Option<R>,
    {
        let mut res = None;
        self.visit_down(&mut |n| {
            res = f(n);
            if res.is_some() {
                Ok(TreeNodeRecursion::StopAll)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })
        .unwrap();
        res
    }

    /// Convenience function to collect all values that `f` returns in a preorder
    /// traversal.
    fn collect<F, R>(&self, f: &mut F) -> Vec<R>
    where
        F: FnMut(&Self) -> Vec<R>,
    {
        let mut res = vec![];
        self.visit_down(&mut |n| {
            res.extend(f(n));
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        res
    }
}

/// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for
/// recursively visiting [`TreeNode`]s.
///
/// [`TreeNodeVisitor`] allows keeping the algorithms separate from the code to traverse
/// the structure of the [`TreeNode`] tree and makes it easier to add new types of tree
/// node and algorithms.
///
/// When passed to [`TreeNode::visit()`], [`TreeNodeVisitor::pre_visit()`] and
/// [`TreeNodeVisitor::post_visit()`] are invoked recursively on an node tree.
/// See [`TreeNodeRecursion`] for more details on how the traversal can be controlled.
///
/// If an [`Err`] result is returned, recursion is stopped immediately.
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked before any inner children or children of a node are visited.
    fn pre_visit(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion>;

    /// Invoked after all inner children and children of a node are visited.
    fn post_visit(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion>;
}

/// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for
/// recursively transforming [`TreeNode`]s.
///
/// When passed to [`TreeNode::transform()`], [`TreeNodeTransformer::pre_transform()`] and
/// [`TreeNodeTransformer::post_transform()`] are invoked recursively on an node tree.
/// See [`TreeNodeRecursion`] for more details on how the traversal can be controlled.
///
/// If an [`Err`] result is returned, recursion is stopped immediately.
pub trait TreeNodeTransformer: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked before any inner children or children of a node are modified.
    fn pre_transform(&mut self, node: &mut Self::Node) -> Result<TreeNodeRecursion>;

    /// Invoked after all inner children and children of a node are modified.
    fn post_transform(&mut self, node: &mut Self::Node) -> Result<TreeNodeRecursion>;
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

/// Controls how a [`TreeNode`] recursion should proceed for [`TreeNode::visit_down()`],
/// [`TreeNode::visit()`], [`TreeNode::transform_down()`], [`TreeNode::transform_up()`]
/// and [`TreeNode::transform()`].
#[derive(Debug)]
pub enum TreeNodeRecursion {
    /// Continue the visit to the next node.
    Continue,

    /// Prune the current subtree.
    /// If a preorder visit of a tree node returns [`TreeNodeRecursion::Prune`] then inner
    /// children and children will not be visited and postorder visit of the node will not
    /// be invoked.
    Prune,

    /// Stop recursion on current tree.
    /// If recursion runs on an inner tree then returning [`TreeNodeRecursion::Stop`] doesn't
    /// stop recursion on the outer tree.
    Stop,

    /// Stop recursion on all (including outer) trees.
    StopAll,
}

impl TreeNodeRecursion {
    /// Helper function to define behavior of a [`TreeNode`] recursion to continue with a
    /// closure if the recursion so far resulted [`TreeNodeRecursion::Continue]`.
    pub fn and_then_on_continue<F>(self, f: F) -> Result<TreeNodeRecursion>
    where
        F: FnOnce() -> Result<TreeNodeRecursion>,
    {
        match self {
            TreeNodeRecursion::Continue => f(),
            o => Ok(o),
        }
    }

    fn continue_on_prune(self) -> Result<TreeNodeRecursion> {
        Ok(match self {
            TreeNodeRecursion::Prune => TreeNodeRecursion::Continue,
            o => o,
        })
    }

    pub fn fail_on_prune(self) -> Result<TreeNodeRecursion> {
        Ok(match self {
            TreeNodeRecursion::Prune => panic!("Recursion can't prune."),
            o => o,
        })
    }

    fn continue_on_stop(self) -> Result<TreeNodeRecursion> {
        Ok(match self {
            TreeNodeRecursion::Stop => TreeNodeRecursion::Continue,
            o => o,
        })
    }
}

pub trait VisitRecursionIterator: Iterator {
    fn for_each_till_continue<F>(self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(Self::Item) -> Result<TreeNodeRecursion>;
}

impl<I: Iterator> VisitRecursionIterator for I {
    fn for_each_till_continue<F>(self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(Self::Item) -> Result<TreeNodeRecursion>,
    {
        for i in self {
            match f(i)? {
                TreeNodeRecursion::Continue => {}
                o => return Ok(o),
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
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
    fn children_nodes(&self) -> Vec<Cow<Self>> {
        self.arc_children().into_iter().map(Cow::Owned).collect()
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children: Result<Vec<_>> =
                children.into_iter().map(transform).collect();
            let arc_self = Arc::clone(&self);
            self.with_new_arc_children(arc_self, new_children?)
        } else {
            Ok(self)
        }
    }

    fn transform_children<F>(&mut self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&mut Self) -> Result<TreeNodeRecursion>,
    {
        let mut new_children = self.arc_children();
        if !new_children.is_empty() {
            let tnr = new_children.iter_mut().for_each_till_continue(f)?;
            *self = self.with_new_arc_children(self.clone(), new_children)?;
            Ok(tnr)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    }
}
