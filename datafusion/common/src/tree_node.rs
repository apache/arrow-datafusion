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

//! [`TreeNode`] for visiting and rewriting expression and plan trees

use std::sync::Arc;

use crate::Result;

/// These macros are used to determine continuation during transforming traversals.
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_CHILD:expr, $F_UP:expr) => {{
        $F_DOWN?
            .transform_children(|n| n.map_children($F_CHILD))?
            .transform_parent($F_UP)
    }};
}

/// API for inspecting and rewriting tree data structures.
///
/// The `TreeNode` API is used to express algorithms separately from traversing
/// the structure of `TreeNode`s, avoiding substantial code duplication.
///
/// This trait is implemented for plans ([`ExecutionPlan`], [`LogicalPlan`]) and
/// expression trees ([`PhysicalExpr`], [`Expr`]) as well as Plan+Payload
/// combinations [`PlanContext`] and [`ExprContext`].
///
/// # Overview
/// There are three categories of TreeNode APIs:
///
/// 1. "Inspecting" APIs to traverse a tree of `&TreeNodes`:
/// [`apply`], [`visit`], [`exists`].
///
/// 2. "Transforming" APIs that traverse and consume a tree of `TreeNode`s
/// producing possibly changed `TreeNode`s: [`transform`], [`transform_up`],
/// [`transform_down`], [`transform_down_up`], and [`rewrite`].
///
/// 3. Internal APIs used to implement the `TreeNode` API: [`apply_children`],
/// and [`map_children`].
///
/// | Traversal Order | Inspecting | Transforming |
/// | --- | --- | --- |
/// | top-down | [`apply`], [`exists`] | [`transform_down`]|
/// | bottom-up | | [`transform`] , [`transform_up`]|
/// | combined with separate `f_down` and `f_up` closures | | [`transform_down_up`] |
/// | combined with `f_down()` and `f_up()` in an object | [`visit`]  | [`rewrite`] |
///
/// **Note**:while there is currently no in-place mutation API that uses `&mut
/// TreeNode`, the transforming APIs are efficient and optimized to avoid
/// cloning.
///
/// [`apply`]: Self::apply
/// [`visit`]: Self::visit
/// [`exists`]: Self::exists
/// [`transform`]: Self::transform
/// [`transform_up`]: Self::transform_up
/// [`transform_down`]: Self::transform_down
/// [`transform_down_up`]: Self::transform_down_up
/// [`rewrite`]: Self::rewrite
/// [`apply_children`]: Self::apply_children
/// [`map_children`]: Self::map_children
///
/// # Terminology
/// The following terms are used in this trait
///
/// * `f_down`: Invoked before any children of the current node are visited.
/// * `f_up`: Invoked after all children of the current node are visited.
/// * `f`: closure that is applied to the current node.
/// * `map_*`: applies a transformation to rewrite owned nodes
/// * `apply_*`:  invokes a function on borrowed nodes
/// * `transform_`: applies a transformation to rewrite owned nodes
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
/// [`PlanContext`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/tree_node/struct.PlanContext.html
/// [`ExprContext`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/tree_node/struct.ExprContext.html
pub trait TreeNode: Sized {
    /// Visit the tree node with a [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeVisitor::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeVisitor::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// # Return Value
    /// Specifies how the tree walk ended. See [`TreeNodeRecursion`] for details.
    ///
    /// # See Also:
    /// * [`Self::apply`] for inspecting nodes with a closure
    /// * [`Self::rewrite`] to rewrite owned `TreeNode`s
    ///
    /// # Example
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
    fn visit<'n, V: TreeNodeVisitor<'n, Node = Self>>(
        &'n self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        visitor
            .f_down(self)?
            .visit_children(|| self.apply_children(|c| c.visit(visitor)))?
            .visit_parent(|| visitor.f_up(self))
    }

    /// Rewrite the tree node with a [`TreeNodeRewriter`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeRewriter::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeRewriter::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// Note: If using the default [`TreeNodeRewriter::f_up`] or
    /// [`TreeNodeRewriter::f_down`] that do nothing, consider using
    /// [`Self::transform_down`] instead.
    ///
    /// # Return Value
    /// The returns value specifies how the tree walk should proceed. See
    /// [`TreeNodeRecursion`] for details. If an [`Err`] is returned, the
    /// recursion stops immediately.
    ///
    /// # See Also
    /// * [`Self::visit`] for inspecting (without modification) `TreeNode`s
    /// * [Self::transform_down_up] for a top-down (pre-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    ///
    /// # Example
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
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(rewriter.f_down(self), |c| c.rewrite(rewriter), |n| {
            rewriter.f_up(n)
        })
    }

    /// Applies `f` to the node then each of its children, recursively (a
    /// top-down, pre-order traversal).
    ///
    /// The return [`TreeNodeRecursion`] controls the recursion and can cause
    /// an early return.
    ///
    /// # See Also
    /// * [`Self::transform_down`] for the equivalent transformation API.
    /// * [`Self::visit`] for both top-down and bottom up traversal.
    fn apply<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        fn apply_impl<'n, N: TreeNode, F: FnMut(&'n N) -> Result<TreeNodeRecursion>>(
            node: &'n N,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node)?.visit_children(|| node.apply_children(|c| apply_impl(c, f)))
        }

        apply_impl(self, &mut f)
    }

    /// Recursively rewrite the node's children and then the node using `f`
    /// (a bottom-up post-order traversal).
    ///
    /// A synonym of [`Self::transform_up`].
    fn transform<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(f)
    }

    /// Recursively rewrite the tree using `f` in a top-down (pre-order)
    /// fashion.
    ///
    /// `f` is applied to the node first, and then its children.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_down<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        fn transform_down_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            f(node)?.transform_children(|n| n.map_children(|c| transform_down_impl(c, f)))
        }

        transform_down_impl(self, &mut f)
    }

    /// Same as [`Self::transform_down`] but with a mutable closure.
    #[deprecated(since = "38.0.0", note = "Use `transform_down` instead")]
    fn transform_down_mut<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: &mut F,
    ) -> Result<Transformed<Self>> {
        self.transform_down(f)
    }

    /// Recursively rewrite the node using `f` in a bottom-up (post-order)
    /// fashion.
    ///
    /// `f` is applied to the node's  children first, and then to the node itself.
    ///
    /// # See Also
    /// * [`Self::transform_down`] top-down (pre-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_up<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        fn transform_up_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            node.map_children(|c| transform_up_impl(c, f))?
                .transform_parent(f)
        }

        transform_up_impl(self, &mut f)
    }

    /// Same as [`Self::transform_up`] but with a mutable closure.
    #[deprecated(since = "38.0.0", note = "Use `transform_up` instead")]
    fn transform_up_mut<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: &mut F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(f)
    }

    /// Transforms the node using `f_down` while traversing the tree top-down
    /// (pre-order), and using `f_up` while traversing the tree bottom-up
    /// (post-order).
    ///
    /// The method behaves the same as calling [`Self::transform_down`] followed
    /// by [`Self::transform_up`] on the same node. Use this method if you want
    /// to start the `f_up` process right where `f_down` jumps. This can make
    /// the whole process faster by reducing the number of `f_up` steps.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    ///
    /// # Example
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
        mut f_down: FD,
        mut f_up: FU,
    ) -> Result<Transformed<Self>> {
        fn transform_down_up_impl<
            N: TreeNode,
            FD: FnMut(N) -> Result<Transformed<N>>,
            FU: FnMut(N) -> Result<Transformed<N>>,
        >(
            node: N,
            f_down: &mut FD,
            f_up: &mut FU,
        ) -> Result<Transformed<N>> {
            handle_transform_recursion!(
                f_down(node),
                |c| transform_down_up_impl(c, f_down, f_up),
                f_up
            )
        }

        transform_down_up_impl(self, &mut f_down, &mut f_up)
    }

    /// Returns true if `f` returns true for any node in the tree.
    ///
    /// Stops recursion as soon as a matching node is found
    fn exists<F: FnMut(&Self) -> Result<bool>>(&self, mut f: F) -> Result<bool> {
        let mut found = false;
        self.apply(|n| {
            Ok(if f(n)? {
                found = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .map(|_| found)
    }

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::map_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to inspect node's children (but not the node
    /// itself).
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::apply_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to rewrite the node's children (but not the node itself).
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

/// A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
/// inspecting [`TreeNode`]s via [`TreeNode::visit`].
///
/// See [`TreeNode`] for more details on available APIs
///
/// When passed to [`TreeNode::visit`], [`TreeNodeVisitor::f_down`] and
/// [`TreeNodeVisitor::f_up`] are invoked recursively on the tree.
/// See [`TreeNodeRecursion`] for more details on controlling the traversal.
///
/// # Return Value
/// The returns value of `f_up` and `f_down` specifies how the tree walk should
/// proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
/// the recursion stops immediately.
///
/// Note: If using the default implementations of [`TreeNodeVisitor::f_up`] or
/// [`TreeNodeVisitor::f_down`] that do nothing, consider using
/// [`TreeNode::apply`] instead.
///
/// # See Also:
/// * [`TreeNode::rewrite`] to rewrite owned `TreeNode`s
pub trait TreeNodeVisitor<'n>: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree, before any children are visited.
    /// Default implementation continues the recursion.
    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked while traversing up the tree after children are visited. Default
    /// implementation continues the recursion.
    fn f_up(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
/// rewriting [`TreeNode`]s via [`TreeNode::rewrite`].
///
/// See [`TreeNode`] for more details on available APIs
///
/// When passed to [`TreeNode::rewrite`], [`TreeNodeRewriter::f_down`] and
/// [`TreeNodeRewriter::f_up`] are invoked recursively on the tree.
/// See [`TreeNodeRecursion`] for more details on controlling the traversal.
///
/// # Return Value
/// The returns value of `f_up` and `f_down` specifies how the tree walk should
/// proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
/// the recursion stops immediately.
///
/// Note: If using the default implementations of [`TreeNodeRewriter::f_up`] or
/// [`TreeNodeRewriter::f_down`] that do nothing, consider using
/// [`TreeNode::transform_up`] or [`TreeNode::transform_down`] instead.
///
/// # See Also:
/// * [`TreeNode::visit`] to inspect borrowed `TreeNode`s
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

impl TreeNodeRecursion {
    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's children.
    pub fn visit_children<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump => Ok(TreeNodeRecursion::Continue),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's sibling.
    pub fn visit_sibling<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => f(),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's parent.
    pub fn visit_parent<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

/// Result of tree walk / transformation APIs
///
/// `Transformed` is a wrapper around the tree node data (e.g. `Expr` or
/// `LogicalPlan`). It is used to indicate whether the node was transformed
/// and how the recursion should proceed.
///
/// [`TreeNode`] API users control the transformation by returning:
/// - The resulting (possibly transformed) node,
/// - `transformed`: flag indicating whether any change was made to the node
/// - `tnr`: [`TreeNodeRecursion`] specifying how to proceed with the recursion.
///
/// At the end of the transformation, the return value will contain:
/// - The final (possibly transformed) tree,
/// - `transformed`: flag indicating whether any change was made to the node
/// - `tnr`: [`TreeNodeRecursion`] specifying how the recursion ended.
///
/// See also
/// * [`Transformed::update_data`] to modify the node without changing the `transformed` flag
/// * [`Transformed::map_data`] for fallable operation that return the same type
/// * [`Transformed::transform_data`] to chain fallable transformations
/// * [`TransformedResult`] for working with `Result<Transformed<U>>`
///
/// # Examples
///
/// Use [`Transformed::yes`] and [`Transformed::no`] to signal that a node was
/// rewritten and the recursion should continue:
///
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
///
/// // Create a new `Transformed` object signaling the node was not rewritten
/// let ret = Transformed::no(expr.clone());
/// assert!(!ret.transformed);
///
/// // Create a new `Transformed` object signaling the node was rewritten
/// let ret = Transformed::yes(expr);
/// assert!(ret.transformed)
/// ```
///
/// Access the node within the `Transformed` object:
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
///
/// // `Transformed` object signaling the node was not rewritten
/// let ret = Transformed::no(expr.clone());
/// // Access the inner object using .data
/// assert_eq!(expr, ret.data);
/// ```
///
/// Transform the node within the `Transformed` object.
///
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
/// let ret = Transformed::no(expr.clone())
///   .transform_data(|expr| {
///    // closure returns a result and potentially transforms the node
///    // in this example, it does transform the node
///    let new_expr = make_new_expr(expr);
///    Ok(Transformed::yes(new_expr))
///  }).unwrap();
/// // transformed flag is the union of the original ans closure's  transformed flag
/// assert!(ret.transformed);
/// ```
/// # Example APIs that use `TreeNode`
/// - [`TreeNode`],
/// - [`TreeNode::rewrite`],
/// - [`TreeNode::transform_down`],
/// - [`TreeNode::transform_up`],
/// - [`TreeNode::transform_down_up`]
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

    /// Create a `Transformed` with `transformed and [`TreeNodeRecursion::Continue`].
    pub fn new_transformed(data: T, transformed: bool) -> Self {
        Self::new(data, transformed, TreeNodeRecursion::Continue)
    }

    /// Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn yes(data: T) -> Self {
        Self::new(data, true, TreeNodeRecursion::Continue)
    }

    /// Wrapper for unchanged data with [`TreeNodeRecursion::Continue`] statement.
    pub fn no(data: T) -> Self {
        Self::new(data, false, TreeNodeRecursion::Continue)
    }

    /// Applies an infallible `f` to the data of this [`Transformed`] object,
    /// without modifying the `transformed` flag.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed::new(f(self.data), self.transformed, self.tnr)
    }

    /// Applies a fallible `f` (returns `Result`) to the data of this
    /// [`Transformed`] object, without modifying the `transformed` flag.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed::new(data, self.transformed, self.tnr))
    }

    /// Applies a fallible transforming `f` to the data of this [`Transformed`]
    /// object.
    ///
    /// The returned `Transformed` object has the `transformed` flag set if either
    /// `self` or the return value of `f` have the `transformed` flag set.
    pub fn transform_data<U, F: FnOnce(T) -> Result<Transformed<U>>>(
        self,
        f: F,
    ) -> Result<Transformed<U>> {
        f(self.data).map(|mut t| {
            t.transformed |= self.transformed;
            t
        })
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's children.
    pub fn transform_children<F: FnOnce(T) -> Result<Transformed<T>>>(
        mut self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {
                return f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                });
            }
            TreeNodeRecursion::Jump => {
                self.tnr = TreeNodeRecursion::Continue;
            }
            TreeNodeRecursion::Stop => {}
        }
        Ok(self)
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's sibling.
    pub fn transform_sibling<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                })
            }
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's parent.
    pub fn transform_parent<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

/// Transformation helper to process a sequence of iterable tree nodes that are siblings.
pub trait TreeNodeIterator: Iterator {
    /// Apples `f` to each item in this iterator
    ///
    /// Visits all items in the iterator unless
    /// `f` returns an error or `f` returns `TreeNodeRecursion::Stop`.
    ///
    /// # Returns
    /// Error if `f` returns an error or `Ok(TreeNodeRecursion)` from the last invocation
    /// of `f` or `Continue` if the iterator is empty
    fn apply_until_stop<F: FnMut(Self::Item) -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Apples `f` to each item in this iterator
    ///
    /// Visits all items in the iterator unless
    /// `f` returns an error or `f` returns `TreeNodeRecursion::Stop`.
    ///
    /// # Returns
    /// Error if `f` returns an error
    ///
    /// Ok(Transformed) such that:
    /// 1. `transformed` is true if any return from `f` had transformed true
    /// 2. `data` from the last invocation of `f`
    /// 3. `tnr` from the last invocation of `f` or `Continue` if the iterator is empty
    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        f: F,
    ) -> Result<Transformed<Vec<Self::Item>>>;
}

impl<I: Iterator> TreeNodeIterator for I {
    fn apply_until_stop<F: FnMut(Self::Item) -> Result<TreeNodeRecursion>>(
        self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for i in self {
            tnr = f(i)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        mut f: F,
    ) -> Result<Transformed<Vec<Self::Item>>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.map(|item| match tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                f(item).map(|result| {
                    tnr = result.tnr;
                    transformed |= result.transformed;
                    result.data
                })
            }
            TreeNodeRecursion::Stop => Ok(item),
        })
        .collect::<Result<Vec<_>>>()
        .map(|data| Transformed::new(data, transformed, tnr))
    }
}

/// Transformation helper to process a heterogeneous sequence of tree node containing
/// expressions.
///
/// This macro is very similar to [TreeNodeIterator::map_until_stop_and_collect] to
/// process nodes that are siblings, but it accepts an initial transformation (`F0`) and
/// a sequence of pairs. Each pair is made of an expression (`EXPR`) and its
/// transformation (`F`).
///
/// The macro builds up a tuple that contains `Transformed.data` result of `F0` as the
/// first element and further elements from the sequence of pairs. An element from a pair
/// is either the value of `EXPR` or the `Transformed.data` result of `F`, depending on
/// the `Transformed.tnr` result of previous `F`s (`F0` initially).
///
/// # Returns
/// Error if any of the transformations returns an error
///
/// Ok(Transformed<(data0, ..., dataN)>) such that:
/// 1. `transformed` is true if any of the transformations had transformed true
/// 2. `(data0, ..., dataN)`, where `data0` is the `Transformed.data` from `F0` and
///     `data1` ... `dataN` are from either `EXPR` or the `Transformed.data` of `F`
/// 3. `tnr` from `F0` or the last invocation of `F`
#[macro_export]
macro_rules! map_until_stop_and_collect {
    ($F0:expr, $($EXPR:expr, $F:expr),*) => {{
        $F0.and_then(|Transformed { data: data0, mut transformed, mut tnr }| {
            let all_datas = (
                data0,
                $(
                    if tnr == TreeNodeRecursion::Continue || tnr == TreeNodeRecursion::Jump {
                        $F.map(|result| {
                            tnr = result.tnr;
                            transformed |= result.transformed;
                            result.data
                        })?
                    } else {
                        $EXPR
                    },
                )*
            );
            Ok(Transformed::new(all_datas, transformed, tnr))
        })
    }}
}

/// Transformation helper to access [`Transformed`] fields in a [`Result`] easily.
///
/// # Example
/// Access the internal data of a `Result<Transformed<T>>`
/// as a `Result<T>` using the `data` method:
/// ```
/// # use datafusion_common::Result;
/// # use datafusion_common::tree_node::{Transformed, TransformedResult};
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn update_expr() -> i64 { 1 }
/// # fn main() -> Result<()> {
/// let transformed: Result<Transformed<_>> = Ok(Transformed::yes(update_expr()));
/// // access the internal data of the transformed result, or return the error
/// let transformed_expr = transformed.data()?;
/// # Ok(())
/// # }
/// ```
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
    fn arc_children(&self) -> Vec<&Arc<Self>>;

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
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.arc_children().into_iter().apply_until_stop(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children = children
                .into_iter()
                .cloned()
                .map_until_stop_and_collect(f)?;
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
    fn children(&self) -> &[Self];

    /// Detaches the node from its children, returning the node itself and its detached children.
    fn take_children(self) -> (Self, Vec<Self>);

    /// Reattaches updated child nodes to the node, returning the updated node.
    fn with_new_children(self, children: Vec<Self>) -> Result<Self>;
}

impl<T: ConcreteTreeNode> TreeNode for T {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.children().iter().apply_until_stop(f)
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
    use std::collections::HashMap;
    use std::fmt::Display;

    use crate::tree_node::{
        Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion, TreeNodeRewriter,
        TreeNodeVisitor,
    };
    use crate::Result;

    #[derive(Debug, Eq, Hash, PartialEq)]
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
        fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
            &'n self,
            f: F,
        ) -> Result<TreeNodeRecursion> {
            self.children.iter().apply_until_stop(f)
        }

        fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
            self,
            f: F,
        ) -> Result<Transformed<Self>> {
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

    impl<'n, T: Display> TreeNodeVisitor<'n> for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
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
                tree.apply(|node| {
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
                assert_eq!(tree.transform_down_up($F_DOWN, $F_UP,)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_down_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_down($F)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_up_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_up($F)?, $EXPECTED_TREE);

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

    //             F
    //          /  |  \
    //       /     |     \
    //    E        C        A
    //    |      /   \
    //    C     B     D
    //  /   \         |
    // B     D        A
    //       |
    //       A
    #[test]
    fn test_apply_and_visit_references() -> Result<()> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_a_2 = TestTreeNode::new(vec![], "a".to_string());
        let node_b_2 = TestTreeNode::new(vec![], "b".to_string());
        let node_d_2 = TestTreeNode::new(vec![node_a_2], "d".to_string());
        let node_c_2 = TestTreeNode::new(vec![node_b_2, node_d_2], "c".to_string());
        let node_a_3 = TestTreeNode::new(vec![], "a".to_string());
        let tree = TestTreeNode::new(vec![node_e, node_c_2, node_a_3], "f".to_string());

        let node_f_ref = &tree;
        let node_e_ref = &node_f_ref.children[0];
        let node_c_ref = &node_e_ref.children[0];
        let node_b_ref = &node_c_ref.children[0];
        let node_d_ref = &node_c_ref.children[1];
        let node_a_ref = &node_d_ref.children[0];

        let mut m: HashMap<&TestTreeNode<String>, usize> = HashMap::new();
        tree.apply(|e| {
            *m.entry(e).or_insert(0) += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;

        let expected = HashMap::from([
            (node_f_ref, 1),
            (node_e_ref, 1),
            (node_c_ref, 2),
            (node_d_ref, 2),
            (node_b_ref, 2),
            (node_a_ref, 3),
        ]);
        assert_eq!(m, expected);

        struct TestVisitor<'n> {
            m: HashMap<&'n TestTreeNode<String>, (usize, usize)>,
        }

        impl<'n> TreeNodeVisitor<'n> for TestVisitor<'n> {
            type Node = TestTreeNode<String>;

            fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
                let (down_count, _) = self.m.entry(node).or_insert((0, 0));
                *down_count += 1;
                Ok(TreeNodeRecursion::Continue)
            }

            fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
                let (_, up_count) = self.m.entry(node).or_insert((0, 0));
                *up_count += 1;
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut visitor = TestVisitor { m: HashMap::new() };
        tree.visit(&mut visitor)?;

        let expected = HashMap::from([
            (node_f_ref, (1, 1)),
            (node_e_ref, (1, 1)),
            (node_c_ref, (2, 2)),
            (node_d_ref, (2, 2)),
            (node_b_ref, (2, 2)),
            (node_a_ref, (3, 3)),
        ]);
        assert_eq!(visitor.m, expected);

        Ok(())
    }
}
