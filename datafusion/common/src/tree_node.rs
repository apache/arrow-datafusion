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

use crate::{error::_not_impl_err, Result};

/// This macro is used to control continuation behaviors during tree traversals
/// based on the specified direction. Depending on `$DIRECTION` and the value of
/// the given expression (`$EXPR`), which should be a variant of [`TreeNodeRecursion`],
/// the macro results in the following behavior:
///
/// - If the expression returns [`TreeNodeRecursion::Continue`], normal execution
///   continues.
/// - If it returns [`TreeNodeRecursion::Stop`], recursion halts and propagates
///   [`TreeNodeRecursion::Stop`].
/// - If it returns [`TreeNodeRecursion::Jump`], the continuation behavior depends
///   on the traversal direction:
///   - For `UP` direction, the function returns with [`TreeNodeRecursion::Jump`],
///     bypassing further bottom-up closures until the next top-down closure.
///   - For `DOWN` direction, the function returns with [`TreeNodeRecursion::Continue`],
///     skipping further exploration.
///   - If no direction is specified, `Jump` is treated like `Continue`.
#[macro_export]
macro_rules! handle_visit_recursion {
    // Internal helper macro for handling the `Jump` case based on the direction:
    (@handle_jump UP) => {
        return Ok(TreeNodeRecursion::Jump)
    };
    (@handle_jump DOWN) => {
        return Ok(TreeNodeRecursion::Continue)
    };
    (@handle_jump) => {
        {} // Treat `Jump` like `Continue`, do nothing and continue execution.
    };

    // Main macro logic with variables to handle directionality.
    ($EXPR:expr $(, $DIRECTION:ident)?) => {
        match $EXPR {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => handle_visit_recursion!(@handle_jump $($DIRECTION)?),
            TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
        }
    };
}

/// This macro is used to determine continuation during combined transforming
/// traversals.
///
/// Depending on the [`TreeNodeRecursion`] the bottom-up closure returns,
/// [`Transformed::try_transform_node_with()`] decides recursion continuation
/// and if state propagation is necessary. Then, the same procedure recursively
/// applies to the children of the node in question.
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_SELF:expr, $F_UP:expr) => {{
        let pre_visited = $F_DOWN?;
        match pre_visited.tnr {
            TreeNodeRecursion::Continue => pre_visited
                .data
                .map_children($F_SELF)?
                .try_transform_node_with($F_UP, TreeNodeRecursion::Jump),
            #[allow(clippy::redundant_closure_call)]
            TreeNodeRecursion::Jump => $F_UP(pre_visited.data),
            TreeNodeRecursion::Stop => return Ok(pre_visited),
        }
        .map(|mut post_visited| {
            post_visited.transformed |= pre_visited.transformed;
            post_visited
        })
    }};
}

/// Defines a visitable and rewriteable tree node. This trait is implemented
/// for plans ([`ExecutionPlan`] and [`LogicalPlan`]) as well as expression
/// trees ([`PhysicalExpr`], [`Expr`]) in DataFusion.
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
pub trait TreeNode: Sized {
    /// Visit the tree node using the given [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeVisitor::f_down(ParentNode)
    /// TreeNodeVisitor::f_down(ChildNode1)
    /// TreeNodeVisitor::f_up(ChildNode1)
    /// TreeNodeVisitor::f_down(ChildNode2)
    /// TreeNodeVisitor::f_up(ChildNode2)
    /// TreeNodeVisitor::f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// If [`TreeNodeVisitor::f_down()`] or [`TreeNodeVisitor::f_up()`] returns [`Err`],
    /// the recursion stops immediately.
    ///
    /// If using the default [`TreeNodeVisitor::f_up`] that does nothing, consider using
    /// [`Self::apply`].
    fn visit<V: TreeNodeVisitor<Node = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        match visitor.f_down(self)? {
            TreeNodeRecursion::Continue => {
                handle_visit_recursion!(
                    self.apply_children(&mut |n| n.visit(visitor))?,
                    UP
                );
                visitor.f_up(self)
            }
            TreeNodeRecursion::Jump => visitor.f_up(self),
            TreeNodeRecursion::Stop => Ok(TreeNodeRecursion::Stop),
        }
    }

    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for
    /// recursively transforming [`TreeNode`]s.
    ///
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeRewriter::f_down(ParentNode)
    /// TreeNodeRewriter::f_down(ChildNode1)
    /// TreeNodeRewriter::f_up(ChildNode1)
    /// TreeNodeRewriter::f_down(ChildNode2)
    /// TreeNodeRewriter::f_up(ChildNode2)
    /// TreeNodeRewriter::f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// If [`TreeNodeVisitor::f_down()`] or [`TreeNodeVisitor::f_up()`] returns [`Err`],
    /// the recursion stops immediately.
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(rewriter.f_down(self), |c| c.rewrite(rewriter), |n| {
            rewriter.f_up(n)
        })
    }

    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for
    /// recursively mutating / rewriting [`TreeNode`]s in place
    ///
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be mutated using the following order:
    /// ```text
    /// TreeNodeMutator::f_down(ParentNode)
    /// TreeNodeMutator::f_down(ChildNode1)
    /// TreeNodeMutator::f_up(ChildNode1)
    /// TreeNodeMutator::f_down(ChildNode2)
    /// TreeNodeMutator::f_up(ChildNode2)
    /// TreeNodeMutator::f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// # Error Handling
    ///
    /// If [`TreeNodeVisitor::f_down()`] or [`TreeNodeVisitor::f_up()`] returns [`Err`],
    /// the recursion stops immediately and the tree may be left partially changed
    ///
    /// # Changing Children During Traversal
    ///
    /// If `f_down` changes the nodes children, the new children are visited
    /// (not the old children prior to rewrite)
    fn mutate<M: TreeNodeMutator<Node = Self>>(
        &mut self,
        mutator: &mut M,
    ) -> Result<Transformed<()>> {
        // Note this is an inlined version of handle_transform_recursion!
        let pre_visited = mutator.f_down(self)?;

        // Traverse children and then call f_up on self if necessary
        match pre_visited.tnr {
            TreeNodeRecursion::Continue => {
                // recurse to children and then apply f_up to self if needed
                self.mutate_children(|c| mutator.f_down(c))?
                    .try_transform_node_with(
                        |_: ()| mutator.f_up(self),
                        TreeNodeRecursion::Jump,
                    )
            }
            TreeNodeRecursion::Jump => {
                // skip other children and start back up
                mutator.f_up(self)
            }
            TreeNodeRecursion::Stop => return Ok(pre_visited),
        }
        .map(|mut post_visited| {
            post_visited.transformed |= pre_visited.transformed;
            post_visited
        })
    }

    /// Applies `f` to the node and its children. `f` is applied in a pre-order
    /// way, and it is controlled by [`TreeNodeRecursion`], which means result
    /// of the `f` on a node can cause an early return.
    ///
    /// The `f` closure can be used to collect some information from tree nodes
    /// or run a check on the tree.
    fn apply<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        handle_visit_recursion!(f(self)?, DOWN);
        self.apply_children(&mut |n| n.apply(f))
    }

    /// Convenience utility for writing optimizer rules: Recursively apply the
    /// given function `f` to the tree in a bottom-up (post-order) fashion. When
    /// `f` does not apply to a given node, it is left unchanged.
    fn transform<F: Fn(Self) -> Result<Transformed<Self>>>(
        self,
        f: &F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(f)
    }

    /// Convenience utility for writing optimizer rules: Recursively apply the
    /// given function `f` to a node and then to its children (pre-order traversal).
    /// When `f` does not apply to a given node, it is left unchanged.
    fn transform_down<F: Fn(Self) -> Result<Transformed<Self>>>(
        self,
        f: &F,
    ) -> Result<Transformed<Self>> {
        f(self)?.try_transform_node_with(
            |n| n.map_children(|c| c.transform_down(f)),
            TreeNodeRecursion::Continue,
        )
    }

    /// Convenience utility for writing optimizer rules: Recursively apply the
    /// given mutable function `f` to a node and then to its children (pre-order
    /// traversal). When `f` does not apply to a given node, it is left unchanged.
    fn transform_down_mut<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: &mut F,
    ) -> Result<Transformed<Self>> {
        f(self)?.try_transform_node_with(
            |n| n.map_children(|c| c.transform_down_mut(f)),
            TreeNodeRecursion::Continue,
        )
    }

    /// Convenience utility for writing optimizer rules: Recursively apply the
    /// given function `f` to all children of a node, and then to the node itself
    /// (post-order traversal). When `f` does not apply to a given node, it is
    /// left unchanged.
    fn transform_up<F: Fn(Self) -> Result<Transformed<Self>>>(
        self,
        f: &F,
    ) -> Result<Transformed<Self>> {
        self.map_children(|c| c.transform_up(f))?
            .try_transform_node_with(f, TreeNodeRecursion::Jump)
    }

    /// Convenience utility for writing optimizer rules: Recursively apply the
    /// given mutable function `f` to all children of a node, and then to the
    /// node itself (post-order traversal). When `f` does not apply to a given
    /// node, it is left unchanged.
    fn transform_up_mut<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: &mut F,
    ) -> Result<Transformed<Self>> {
        self.map_children(|c| c.transform_up_mut(f))?
            .try_transform_node_with(f, TreeNodeRecursion::Jump)
    }

    /// Transforms the tree using `f_down` while traversing the tree top-down
    /// (pre-order), and using `f_up` while traversing the tree bottom-up
    /// (post-order).
    ///
    /// Use this method if you want to start the `f_up` process right where `f_down` jumps.
    /// This can make the whole process faster by reducing the number of `f_up` steps.
    /// If you don't need this, it's just like using `transform_down_mut` followed by
    /// `transform_up_mut` on the same tree.
    ///
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order:
    /// ```text
    /// f_down(ParentNode)
    /// f_down(ChildNode1)
    /// f_up(ChildNode1)
    /// f_down(ChildNode2)
    /// f_up(ChildNode2)
    /// f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// If `f_down` or `f_up` returns [`Err`], the recursion stops immediately.
    ///
    /// Example:
    /// ```text
    ///                                               |   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                  TreeNodeRecursion::Continue  |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                              \|/  | F |
    ///                                               '   +---+
    ///                                                  /     \ ___________________
    ///                  When `f_down` is           +---+                           \ ---+
    ///                  applied on node "E",       | E |                            | G |
    ///                  it returns with "Jump".    +---+                            +---+
    ///                                               |                                |
    ///                                             +---+                            +---+
    ///                                             | C |                            | H |
    ///                                             +---+                            +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    ///
    /// Instead of starting from leaf nodes, `f_up` starts from the node "E".
    ///                                                   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                               |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                              /    +---+
    ///                                            /      | F |
    ///                                          /        +---+
    ///                                        /         /     \ ______________________
    ///                                       |     +---+   .                          \ ---+
    ///                                       |     | E |  /|\  After `f_down` jumps    | G |
    ///                                       |     +---+   |   on node E, `f_up`       +---+
    ///                                        \------| ---/   if applied on node E.      |
    ///                                             +---+                               +---+
    ///                                             | C |                               | H |
    ///                                             +---+                               +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    /// ```
    fn transform_down_up<
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    >(
        self,
        f_down: &mut FD,
        f_up: &mut FU,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(
            f_down(self),
            |c| c.transform_down_up(f_down, f_up),
            f_up
        )
    }

    /// Apply the closure `F` to the node's children.
    ///
    /// See `mutate_children` for rewriting in place
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion>;

    /// Rewrite the node's children in place using `F`.
    ///
    /// Using [`Self::map_children`], the owned API, is more ideomatic and
    /// has clearer semantics on error (the node is consumed). However, it requires
    /// copying the interior fields of the tree node during rewrite
    ///
    /// This API writes the nodes in place, which can be faster as it avoids
    /// copying. However, one downside is that the tree node can be left in an
    /// partially rewritten state when an error occurs.
    fn mutate_children<F: FnMut(&mut Self) -> Result<Transformed<()>>>(
        &mut self,
        _f: F,
    ) -> Result<Transformed<()>> {
        _not_impl_err!(
            "mutate_children not implemented for {} yet",
            std::any::type_name::<Self>()
        )
    }

    /// Apply transform `F` to potentially rewrite the node's children. Note
    /// that the transform `F` might have a direction (pre-order or post-order).
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

/// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern)
/// for recursively walking [`TreeNode`]s.
///
/// A [`TreeNodeVisitor`] allows one to express algorithms separately from the
/// code traversing the structure of the `TreeNode` tree, making it easier to
/// add new types of tree nodes and algorithms.
///
/// When passed to [`TreeNode::visit`], [`TreeNodeVisitor::f_down`] and
/// [`TreeNodeVisitor::f_up`] are invoked recursively on the tree.
/// See [`TreeNodeRecursion`] for more details on controlling the traversal.
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked before any children of `node` are visited.
    /// Default implementation simply continues the recursion.
    fn f_down(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked after all children of `node` are visited.
    /// Default implementation simply continues the recursion.
    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Trait for potentially recursively transforming a tree of [`TreeNode`]s.
pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    /// Invoked while traversing up the tree after all children have been rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

/// Trait for potentially rewriting tree of [`TreeNode`]s in place
///
/// See [`TreeNodeRewriter`] for rewriting owned tree ndoes
/// See [`TreeNodeVisitor`] for visiting, but not changing, tree nodes
pub trait TreeNodeMutator: Sized {
    /// The node type to rewrite.
    type Node: TreeNode;

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node as is and continues recursion.
    ///
    /// Since this mutates the nodes in place, the returned Transformed object
    /// returns `()` (no data).
    ///
    /// If the node's children are changed by `f_down`, the *new* children are
    /// visited, not the original.
    fn f_down(&mut self, _node: &mut Self::Node) -> Result<Transformed<()>> {
        Ok(Transformed::no(()))
    }

    /// Invoked while traversing up the tree after all children have been rewritten.
    /// Default implementation returns the node as is and continues recursion.
    ///
    /// Since this mutates the nodes in place, the returned Transformed object
    /// returns `()` (no data).
    fn f_up(&mut self, _node: &mut Self::Node) -> Result<Transformed<()>> {
        Ok(Transformed::no(()))
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,
    /// In top-down traversals, skip recursing into children but continue with
    /// the next node, which actually means pruning of the subtree.
    ///
    /// In bottom-up traversals, bypass calling bottom-up closures till the next
    /// leaf node.
    ///
    /// In combined traversals, if it is the `f_down` (pre-order) phase, execution
    /// "jumps" to the next `f_up` (post-order) phase by shortcutting its children.
    /// If it is the `f_up` (post-order) phase, execution "jumps" to the next `f_down`
    /// (pre-order) phase by shortcutting its parent nodes until the first parent node
    /// having unvisited children path.
    Jump,
    /// Stop recursion.
    Stop,
}

/// This struct is used by tree transformation APIs such as
/// - [`TreeNode::rewrite`],
/// - [`TreeNode::transform_down`],
/// - [`TreeNode::transform_down_mut`],
/// - [`TreeNode::transform_up`],
/// - [`TreeNode::transform_up_mut`],
/// - [`TreeNode::transform_down_up`]
///
/// to control the transformation and return the transformed result.
///
/// Specifically, API users can provide transformation closures or [`TreeNodeRewriter`]
/// implementations to control the transformation by returning:
/// - The resulting (possibly transformed) node,
/// - A flag indicating whether any change was made to the node, and
/// - A flag specifying how to proceed with the recursion.
///
/// At the end of the transformation, the return value will contain:
/// - The final (possibly transformed) tree,
/// - A flag indicating whether any change was made to the tree, and
/// - A flag specifying how the recursion ended.
#[derive(PartialEq, Debug)]
pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
    pub tnr: TreeNodeRecursion,
}

impl<T> Transformed<T> {
    /// Create a new `Transformed` object with the given information.
    pub fn new(data: T, transformed: bool, tnr: TreeNodeRecursion) -> Self {
        Self {
            data,
            transformed,
            tnr,
        }
    }

    /// Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn yes(data: T) -> Self {
        Self::new(data, true, TreeNodeRecursion::Continue)
    }

    /// Wrapper for unchanged data with [`TreeNodeRecursion::Continue`] statement.
    pub fn no(data: T) -> Self {
        Self::new(data, false, TreeNodeRecursion::Continue)
    }

    /// Applies the given `f` to the data of this [`Transformed`] object.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed::new(f(self.data), self.transformed, self.tnr)
    }

    /// Maps the data of [`Transformed`] object to the result of the given `f`.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed::new(data, self.transformed, self.tnr))
    }

    /// Invokes f(), depending on the value of self.tnr.
    ///
    /// This is used to conditionally apply a function during a f_up tree
    /// traversal, if the result of children traversal was `Continue`.
    ///
    /// Handling [`TreeNodeRecursion::Continue`] and [`TreeNodeRecursion::Stop`]
    /// is straightforward, but [`TreeNodeRecursion::Jump`] can behave differently
    /// when we are traversing down or up on a tree. If [`TreeNodeRecursion`] of
    /// the node is [`TreeNodeRecursion::Jump`], recursion stops with the given
    /// `return_if_jump` value.
    fn try_transform_node_with<F: FnOnce(T) -> Result<Transformed<T>>>(
        mut self,
        f: F,
        return_if_jump: TreeNodeRecursion,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {
                return f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                });
            }
            TreeNodeRecursion::Jump => {
                self.tnr = return_if_jump;
            }
            TreeNodeRecursion::Stop => {}
        }
        Ok(self)
    }

    /// If [`TreeNodeRecursion`] of the node is [`TreeNodeRecursion::Continue`] or
    /// [`TreeNodeRecursion::Jump`], transformation is applied to the node.
    /// Otherwise, it remains as it is.
    pub fn try_transform_node<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        if self.tnr == TreeNodeRecursion::Stop {
            Ok(self)
        } else {
            f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            })
        }
    }
}

/// Transformation helper to process tree nodes that are siblings.
pub trait TransformedIterator: Iterator {
    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        f: F,
    ) -> Result<Transformed<Vec<Self::Item>>>;
}

impl<I: Iterator> TransformedIterator for I {
    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        mut f: F,
    ) -> Result<Transformed<Vec<Self::Item>>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        let data = self
            .map(|item| match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                    f(item).map(|result| {
                        tnr = result.tnr;
                        transformed |= result.transformed;
                        result.data
                    })
                }
                TreeNodeRecursion::Stop => Ok(item),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed::new(data, transformed, tnr))
    }
}

/// Transformation helper to access [`Transformed`] fields in a [`Result`] easily.
pub trait TransformedResult<T> {
    fn data(self) -> Result<T>;

    fn transformed(self) -> Result<bool>;

    fn tnr(self) -> Result<TreeNodeRecursion>;
}

impl<T> TransformedResult<T> for Result<Transformed<T>> {
    fn data(self) -> Result<T> {
        self.map(|t| t.data)
    }

    fn transformed(self) -> Result<bool> {
        self.map(|t| t.transformed)
    }

    fn tnr(self) -> Result<TreeNodeRecursion> {
        self.map(|t| t.tnr)
    }
}

/// Helper trait for implementing [`TreeNode`] that have children stored as
/// `Arc`s. If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`TreeNode`].
pub trait DynTreeNode {
    /// Returns all children of the specified `TreeNode`.
    fn arc_children(&self) -> Vec<Arc<Self>>;

    /// Constructs a new node with the specified children.
    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

/// Blanket implementation for any `Arc<T>` where `T` implements [`DynTreeNode`]
/// (such as [`Arc<dyn PhysicalExpr>`]).
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for child in self.arc_children() {
            tnr = f(&child)?;
            handle_visit_recursion!(tnr)
        }
        Ok(tnr)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children = children.into_iter().map_until_stop_and_collect(f)?;
            // Propagate up `new_children.transformed` and `new_children.tnr`
            // along with the node containing transformed children.
            if new_children.transformed {
                let arc_self = Arc::clone(&self);
                new_children.map_data(|new_children| {
                    self.with_new_arc_children(arc_self, new_children)
                })
            } else {
                Ok(Transformed::new(self, false, new_children.tnr))
            }
        } else {
            Ok(Transformed::no(self))
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
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for child in self.children() {
            tnr = f(child)?;
            handle_visit_recursion!(tnr)
        }
        Ok(tnr)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let (new_self, children) = self.take_children();
        if !children.is_empty() {
            let new_children = children.into_iter().map_until_stop_and_collect(f)?;
            // Propagate up `new_children.transformed` and `new_children.tnr` along with
            // the node containing transformed children.
            new_children.map_data(|new_children| new_self.with_new_children(new_children))
        } else {
            Ok(Transformed::no(new_self))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use crate::tree_node::{
        Transformed, TransformedIterator, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
        TreeNodeVisitor,
    };
    use crate::Result;

    #[derive(PartialEq, Debug)]
    struct TestTreeNode<T> {
        children: Vec<TestTreeNode<T>>,
        data: T,
    }

    impl<T> TestTreeNode<T> {
        fn new(children: Vec<TestTreeNode<T>>, data: T) -> Self {
            Self { children, data }
        }
    }

    impl<T> TreeNode for TestTreeNode<T> {
        fn apply_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
        where
            F: FnMut(&Self) -> Result<TreeNodeRecursion>,
        {
            let mut tnr = TreeNodeRecursion::Continue;
            for child in &self.children {
                tnr = f(child)?;
                handle_visit_recursion!(tnr);
            }
            Ok(tnr)
        }

        fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
        where
            F: FnMut(Self) -> Result<Transformed<Self>>,
        {
            Ok(self
                .children
                .into_iter()
                .map_until_stop_and_collect(f)?
                .update_data(|new_children| Self {
                    children: new_children,
                    ..self
                }))
        }
    }

    //       J
    //       |
    //       I
    //       |
    //       F
    //     /   \
    //    E     G
    //    |     |
    //    C     H
    //  /   \
    // B     D
    //       |
    //       A
    fn test_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    // Continue on all nodes
    // Expected visits in a combined traversal
    fn all_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // Expected transformed tree after a combined traversal
    fn transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    // Expected transformed tree after a top-down traversal
    fn transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // Expected transformed tree after a bottom-up traversal
    fn transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_up(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_up(j)".to_string())
    }

    // f_down Jump on A node
    fn f_down_jump_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_down Jump on E node
    fn f_down_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_down_jump_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_up Jump on A node
    fn f_up_jump_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_jump_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_up_jump_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_up(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_up(j)".to_string())
    }

    // f_up Jump on E node
    fn f_up_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_jump_on_e_transformed_tree() -> TestTreeNode<String> {
        transformed_tree()
    }

    fn f_up_jump_on_e_transformed_up_tree() -> TestTreeNode<String> {
        transformed_up_tree()
    }

    // f_down Stop on A node

    fn f_down_stop_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_down Stop on E node
    fn f_down_stop_on_e_visits() -> Vec<String> {
        vec!["f_down(j)", "f_down(i)", "f_down(f)", "f_down(e)"]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn f_down_stop_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_up Stop on A node
    fn f_up_stop_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    // f_up Stop on E node
    fn f_up_stop_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_e_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    fn down_visits(visits: Vec<String>) -> Vec<String> {
        visits
            .into_iter()
            .filter(|v| v.starts_with("f_down"))
            .collect()
    }

    type TestVisitorF<T> = Box<dyn FnMut(&TestTreeNode<T>) -> Result<TreeNodeRecursion>>;

    struct TestVisitor<T> {
        visits: Vec<String>,
        f_down: TestVisitorF<T>,
        f_up: TestVisitorF<T>,
    }

    impl<T> TestVisitor<T> {
        fn new(f_down: TestVisitorF<T>, f_up: TestVisitorF<T>) -> Self {
            Self {
                visits: vec![],
                f_down,
                f_up,
            }
        }
    }

    impl<T: Display> TreeNodeVisitor for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_up({})", node.data));
            (*self.f_up)(node)
        }
    }

    fn visit_continue<T>(_: &TestTreeNode<T>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn visit_event_on<T: PartialEq, D: Into<T>>(
        data: D,
        event: TreeNodeRecursion,
    ) -> impl FnMut(&TestTreeNode<T>) -> Result<TreeNodeRecursion> {
        let d = data.into();
        move |node| {
            Ok(if node.data == d {
                event
            } else {
                TreeNodeRecursion::Continue
            })
        }
    }

    macro_rules! visit_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_VISITS:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut visitor = TestVisitor::new(Box::new($F_DOWN), Box::new($F_UP));
                tree.visit(&mut visitor)?;
                assert_eq!(visitor.visits, $EXPECTED_VISITS);

                Ok(())
            }
        };
    }

    macro_rules! test_apply {
        ($NAME:ident, $F:expr, $EXPECTED_VISITS:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut visits = vec![];
                tree.apply(&mut |node| {
                    visits.push(format!("f_down({})", node.data));
                    $F(node)
                })?;
                assert_eq!(visits, $EXPECTED_VISITS);

                Ok(())
            }
        };
    }

    type TestRewriterF<T> =
        Box<dyn FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>>>;

    struct TestRewriter<T> {
        f_down: TestRewriterF<T>,
        f_up: TestRewriterF<T>,
    }

    impl<T> TestRewriter<T> {
        fn new(f_down: TestRewriterF<T>, f_up: TestRewriterF<T>) -> Self {
            Self { f_down, f_up }
        }
    }

    impl<T: Display> TreeNodeRewriter for TestRewriter<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            (*self.f_up)(node)
        }
    }

    fn transform_yes<N: Display, T: Display + From<String>>(
        transformation_name: N,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        move |node| {
            Ok(Transformed::yes(TestTreeNode::new(
                node.children,
                format!("{}({})", transformation_name, node.data).into(),
            )))
        }
    }

    fn transform_and_event_on<
        N: Display,
        T: PartialEq + Display + From<String>,
        D: Into<T>,
    >(
        transformation_name: N,
        data: D,
        event: TreeNodeRecursion,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        let d = data.into();
        move |node| {
            let new_node = TestTreeNode::new(
                node.children,
                format!("{}({})", transformation_name, node.data).into(),
            );
            Ok(if node.data == d {
                Transformed::new(new_node, true, event)
            } else {
                Transformed::yes(new_node)
            })
        }
    }

    macro_rules! rewrite_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut rewriter = TestRewriter::new(Box::new($F_DOWN), Box::new($F_UP));
                assert_eq!(tree.rewrite(&mut rewriter)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(
                    tree.transform_down_up(&mut $F_DOWN, &mut $F_UP,)?,
                    $EXPECTED_TREE
                );

                Ok(())
            }
        };
    }

    macro_rules! transform_down_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_down_mut(&mut $F)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_up_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_up_mut(&mut $F)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    visit_test!(test_visit, visit_continue, visit_continue, all_visits());
    visit_test!(
        test_visit_f_down_jump_on_a,
        visit_event_on("a", TreeNodeRecursion::Jump),
        visit_continue,
        f_down_jump_on_a_visits()
    );
    visit_test!(
        test_visit_f_down_jump_on_e,
        visit_event_on("e", TreeNodeRecursion::Jump),
        visit_continue,
        f_down_jump_on_e_visits()
    );
    visit_test!(
        test_visit_f_up_jump_on_a,
        visit_continue,
        visit_event_on("a", TreeNodeRecursion::Jump),
        f_up_jump_on_a_visits()
    );
    visit_test!(
        test_visit_f_up_jump_on_e,
        visit_continue,
        visit_event_on("e", TreeNodeRecursion::Jump),
        f_up_jump_on_e_visits()
    );
    visit_test!(
        test_visit_f_down_stop_on_a,
        visit_event_on("a", TreeNodeRecursion::Stop),
        visit_continue,
        f_down_stop_on_a_visits()
    );
    visit_test!(
        test_visit_f_down_stop_on_e,
        visit_event_on("e", TreeNodeRecursion::Stop),
        visit_continue,
        f_down_stop_on_e_visits()
    );
    visit_test!(
        test_visit_f_up_stop_on_a,
        visit_continue,
        visit_event_on("a", TreeNodeRecursion::Stop),
        f_up_stop_on_a_visits()
    );
    visit_test!(
        test_visit_f_up_stop_on_e,
        visit_continue,
        visit_event_on("e", TreeNodeRecursion::Stop),
        f_up_stop_on_e_visits()
    );

    test_apply!(test_apply, visit_continue, down_visits(all_visits()));
    test_apply!(
        test_apply_f_down_jump_on_a,
        visit_event_on("a", TreeNodeRecursion::Jump),
        down_visits(f_down_jump_on_a_visits())
    );
    test_apply!(
        test_apply_f_down_jump_on_e,
        visit_event_on("e", TreeNodeRecursion::Jump),
        down_visits(f_down_jump_on_e_visits())
    );
    test_apply!(
        test_apply_f_down_stop_on_a,
        visit_event_on("a", TreeNodeRecursion::Stop),
        down_visits(f_down_stop_on_a_visits())
    );
    test_apply!(
        test_apply_f_down_stop_on_e,
        visit_event_on("e", TreeNodeRecursion::Stop),
        down_visits(f_down_stop_on_e_visits())
    );

    rewrite_test!(
        test_rewrite,
        transform_yes("f_down"),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(f_down_jump_on_e_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_up_jump_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_up_jump_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_up_stop_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_up_stop_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_test!(
        test_transform,
        transform_yes("f_down"),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    transform_test!(
        test_transform_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    transform_test!(
        test_transform_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(f_down_jump_on_e_transformed_tree())
    );
    transform_test!(
        test_transform_f_up_jump_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_tree())
    );
    transform_test!(
        test_transform_f_up_jump_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_tree())
    );
    transform_test!(
        test_transform_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_up_stop_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_up_stop_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_down_test!(
        test_transform_down,
        transform_yes("f_down"),
        Transformed::yes(transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        Transformed::yes(f_down_jump_on_a_transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        Transformed::yes(f_down_jump_on_e_transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        Transformed::new(
            f_down_stop_on_a_transformed_down_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_down_test!(
        test_transform_down_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        Transformed::new(
            f_down_stop_on_e_transformed_down_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_up_test!(
        test_transform_up,
        transform_yes("f_up"),
        Transformed::yes(transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_jump_on_a,
        transform_and_event_on("f_up", "a", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_jump_on_e,
        transform_and_event_on("f_up", "e", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_stop_on_a,
        transform_and_event_on("f_up", "a", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_up_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_up_test!(
        test_transform_up_f_up_stop_on_e,
        transform_and_event_on("f_up", "e", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_up_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
}
