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

/// [`RewriteCycle`] API for executing a sequence of [TreeNodeRewriter]s in multiple passes.
use std::ops::ControlFlow;

use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    Result,
};

/// A builder with methods for executing a "rewrite cycle".
///
/// Often the results of one optimization rule can uncover more optimizations in other optimization
/// rules. A sequence of optimization rules can be ran in multiple "passes" until there are no
/// more optmizations to make.
///
/// The [RewriteCycle] handles logic for running these multi-pass loops.
/// It applies a sequence of [TreeNodeRewriter]s to a [TreeNode] by calling
/// [TreeNode::rewrite] in a loop - passing the output of one rewrite as the input to the next
/// rewrite - until [RewriteCycle::max_cycles] is reached or until every [TreeNode::rewrite]
/// returns a [Transformed::no] result in a consecutive sequence.
#[derive(Debug)]
pub struct RewriteCycle {
    max_cycles: usize,
}

impl Default for RewriteCycle {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteCycle {
    /// The default maximum number of completed cycles to run before terminating the rewrite loop.
    /// You can override this default with [Self::with_max_cycles]
    pub const DEFAULT_MAX_CYCLES: usize = 3;

    /// Creates a new [RewriteCycle] with default options.
    pub fn new() -> Self {
        Self {
            max_cycles: Self::DEFAULT_MAX_CYCLES,
        }
    }
    /// Sets the [Self::max_cycles] to run before terminating the rewrite loop.
    pub fn with_max_cycles(mut self, max_cycles: usize) -> Self {
        self.max_cycles = max_cycles;
        self
    }

    /// The maximum number of completed cycles to run before terminating the rewrite loop.
    /// Defaults to [Self::DEFAULT_MAX_CYCLES].
    pub fn max_cycles(&self) -> usize {
        self.max_cycles
    }

    /// Runs a rewrite cycle on the given [TreeNode] using the given callback function to
    /// explicitly handle the cycle iterations.
    ///
    /// The callback function is given a [RewriteCycleState], which manages the short-circuiting
    /// logic of the loop. The function is expected to call [RewriteCycleState::rewrite] for each
    /// individual [TreeNodeRewriter] in the cycle. [RewriteCycleState::rewrite] returns a [RewriteCycleControlFlow]
    /// result, indicating whether the loop should break or continue.
    ///
    /// ```rust
    /// use datafusion_common::{
    ///     tree_node::{Transformed, TreeNodeRewriter},
    ///     Result, ScalarValue
    /// };
    /// use datafusion_expr::{lit, BinaryExpr, Expr, Operator};
    ///
    /// use datafusion_optimizer::rewrite_cycle::RewriteCycle;
    ///
    /// ///Rewrites a BinaryExpr with operator `op` using a function `f`
    /// struct ConstBinaryExprRewriter {
    ///     op: Operator,
    ///     f: Box<dyn Fn(&ScalarValue, &ScalarValue) -> Result<Transformed<Expr>>>,
    /// }
    /// impl TreeNodeRewriter for ConstBinaryExprRewriter {
    ///     type Node = Expr;
    ///     /// Rewrites BinaryExpr using the function
    ///     fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
    ///         match node {
    ///             Expr::BinaryExpr(BinaryExpr {
    ///                 ref left,
    ///                 ref right,
    ///                 op,
    ///             }) if op == self.op => match (left.as_ref(), right.as_ref()) {
    ///                 (Expr::Literal(left), Expr::Literal(right)) => {
    ///                     Ok((self.f)(left, right)?)
    ///                 }
    ///                 _ => Ok(Transformed::no(node)),
    ///             },
    ///             _ => Ok(Transformed::no(node)),
    ///         }
    ///     }
    /// }
    /// // create two rewriters for evaluating literals
    /// // first rewriter evaluates addition expressions
    /// let mut addition_rewriter = ConstBinaryExprRewriter {
    ///     op: Operator::Plus,
    ///     f: Box::new(|left, right| {
    ///         Ok(Transformed::yes(Expr::Literal(left.add(right)?)))
    ///     }),
    /// };
    /// // second rewriter evaluates multiplication expression
    /// let mut multiplication_rewriter = ConstBinaryExprRewriter {
    ///     op: Operator::Multiply,
    ///     f: Box::new(|left, right| {
    ///         Ok(Transformed::yes(Expr::Literal(left.mul(right)?)))
    ///     }),
    /// };
    /// // Create an expression from constant literals
    /// let expr = lit(6) + (lit(4) * (lit(2) + (lit(3) * lit(5))));
    /// // Run rewriters in a loop until constant expression is fully evaluated
    /// let (evaluated_expr, info) = RewriteCycle::new()
    ///     .with_max_cycles(4)
    ///     .each_cycle(expr, |cycle_state| {
    ///         cycle_state
    ///             .rewrite(&mut addition_rewriter)?
    ///             .rewrite(&mut multiplication_rewriter)
    ///     })
    ///     .unwrap();
    /// assert_eq!(evaluated_expr, lit(74));
    /// assert_eq!(info.completed_cycles(), 3);
    /// assert_eq!(info.total_iterations(), 7);
    /// ```
    pub fn each_cycle<
        Node: TreeNode,
        F: FnMut(
            RewriteCycleState<Node>,
        ) -> RewriteCycleControlFlow<RewriteCycleState<Node>>,
    >(
        &self,
        node: Node,
        mut f: F,
    ) -> Result<(Node, RewriteCycleInfo)> {
        let mut state = RewriteCycleState::new(node);
        if self.max_cycles == 0 {
            return state.finish();
        }
        // run first cycle then record number of rewriters
        state = match f(state) {
            ControlFlow::Break(result) => return result?.finish(),
            ControlFlow::Continue(node) => node,
        };
        state.record_cycle_length();
        if state.is_done() {
            return state.finish();
        }
        // run remaining cycles
        match (1..self.max_cycles).try_fold(state, |state, _| f(state)) {
            ControlFlow::Break(result) => result?.finish(),
            ControlFlow::Continue(state) => state.finish(),
        }
    }
}

/// Iteration state of a rewrite cycle. See [RewriteCycle::each_cycle] for usage examples and information.
#[derive(Debug)]
pub struct RewriteCycleState<Node: TreeNode> {
    node: Node,
    consecutive_unchanged_count: usize,
    rewrite_count: usize,
    cycle_length: Option<usize>,
}

impl<Node: TreeNode> RewriteCycleState<Node> {
    fn new(node: Node) -> Self {
        Self {
            node,
            cycle_length: None,
            consecutive_unchanged_count: 0,
            rewrite_count: 0,
        }
    }

    /// Records the rewrite cycle length based on the current iteration count
    ///
    /// When the total number of writers is not known upfront - such as when using
    /// [RewriteCycle::each_cycle] we need to keep count of the number of [Self::rewrite]
    /// calls and then record the number at the end of the first cycle.
    fn record_cycle_length(&mut self) {
        self.cycle_length = Some(self.rewrite_count);
    }

    /// Returns true when the loop has reached the maximum cycle length or when we've received
    /// consecutive unchanged tree nodes equal to the total number of rewriters.
    fn is_done(&self) -> bool {
        // default value indicates we have not completed a cycle
        let Some(cycle_length) = self.cycle_length else {
            return false;
        };
        self.consecutive_unchanged_count >= cycle_length
    }

    /// Finishes the iteration by consuming the state and returning a [TreeNode] and
    /// [RewriteCycleInfo]
    fn finish(self) -> Result<(Node, RewriteCycleInfo)> {
        Ok((
            self.node,
            RewriteCycleInfo {
                cycle_length: self.cycle_length.unwrap_or(self.rewrite_count),
                total_iterations: self.rewrite_count,
            },
        ))
    }

    /// Calls [TreeNode::rewrite] and determines if the rewrite cycle should break or continue
    /// based on the current [RewriteCycleState].
    pub fn rewrite<R: TreeNodeRewriter<Node = Node> + ?Sized>(
        mut self,
        rewriter: &mut R,
    ) -> RewriteCycleControlFlow<Self> {
        match self.node.rewrite(rewriter) {
            Err(e) => ControlFlow::Break(Err(e)),
            Ok(Transformed {
                data: node,
                transformed,
                ..
            }) => {
                self.node = node;
                self.rewrite_count += 1;
                if transformed {
                    self.consecutive_unchanged_count = 0;
                } else {
                    self.consecutive_unchanged_count += 1;
                }
                if self.is_done() {
                    ControlFlow::Break(Ok(self))
                } else {
                    ControlFlow::Continue(self)
                }
            }
        }
    }
}

/// Information about a rewrite cycle, such as total number of iterations and number of fully
/// completed cycles. This is useful for testing purposes to ensure that optimzation passes are
/// working as expected.
#[derive(Debug, Clone, Copy)]
pub struct RewriteCycleInfo {
    total_iterations: usize,
    cycle_length: usize,
}

impl RewriteCycleInfo {
    /// The total number of **fully completed** cycles.
    pub fn completed_cycles(&self) -> usize {
        self.total_iterations / self.cycle_length
    }

    /// The total number of [TreeNode::rewrite] calls.
    pub fn total_iterations(&self) -> usize {
        self.total_iterations
    }

    /// The number of [TreeNode::rewrite] calls within a single cycle.
    pub fn cycle_length(&self) -> usize {
        self.cycle_length
    }
}

pub type RewriteCycleControlFlow<T> = ControlFlow<Result<T>, T>;
#[cfg(test)]
mod test {
    use datafusion_common::{
        tree_node::{Transformed, TreeNodeRewriter},
        Result, ScalarValue,
    };
    use datafusion_expr::{lit, BinaryExpr, Expr, Operator};

    use crate::rewrite_cycle::RewriteCycle;

    /// Rewriter that does not make any change
    struct IdentityRewriter {}
    impl TreeNodeRewriter for IdentityRewriter {
        type Node = Expr;
        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            Ok(Transformed::no(node))
        }
    }

    /// Rewriter that always sets transformed=true
    struct AlwaysTransformedRewriter {}
    impl TreeNodeRewriter for AlwaysTransformedRewriter {
        type Node = Expr;
        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            Ok(Transformed::yes(node))
        }
    }

    ///Rewrites a BinaryExpr with operator `op` using a function `f`
    #[allow(clippy::type_complexity)]
    struct ConstBinaryExprRewriter {
        op: Operator,
        f: Box<dyn Fn(&ScalarValue, &ScalarValue) -> Result<Transformed<Expr>>>,
    }
    impl TreeNodeRewriter for ConstBinaryExprRewriter {
        type Node = Expr;
        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            match node {
                Expr::BinaryExpr(BinaryExpr {
                    ref left,
                    ref right,
                    op,
                }) if op == self.op => match (left.as_ref(), right.as_ref()) {
                    (Expr::Literal(left), Expr::Literal(right)) => {
                        Ok((self.f)(left, right)?)
                    }
                    _ => Ok(Transformed::no(node)),
                },
                _ => Ok(Transformed::no(node)),
            }
        }
    }

    #[test]
    // cycle that makes no changes should complete exactly one cycle
    fn rewrite_cycle_identity() {
        let expr = lit(true);
        let (expr, info) = RewriteCycle::new()
            .with_max_cycles(50)
            .each_cycle(expr, |cycle_state| {
                cycle_state
                    .rewrite(&mut IdentityRewriter {})?
                    .rewrite(&mut IdentityRewriter {})?
                    .rewrite(&mut IdentityRewriter {})
            })
            .unwrap();
        assert_eq!(expr, lit(true));
        assert_eq!(info.completed_cycles(), 1);
        assert_eq!(info.total_iterations(), 3);
    }

    // rewriter that always transforms should complete all cycles
    #[test]
    fn rewrite_cycle_always_transforms() {
        let expr = lit(true);
        let (expr, info) = RewriteCycle::new()
            .with_max_cycles(10)
            .each_cycle(expr, |cycle_state| {
                cycle_state
                    .rewrite(&mut IdentityRewriter {})?
                    .rewrite(&mut AlwaysTransformedRewriter {})
            })
            .unwrap();
        assert_eq!(expr, lit(true));
        assert_eq!(info.completed_cycles(), 10);
        assert_eq!(info.total_iterations(), 20);
    }

    #[test]
    // test an example of const evaluation with two rewriters that depend on each other
    fn rewrite_cycle_const_evaluation() {
        let mut addition_rewriter = ConstBinaryExprRewriter {
            op: Operator::Plus,
            f: Box::new(|left, right| {
                Ok(Transformed::yes(Expr::Literal(left.add(right)?)))
            }),
        };
        let mut multiplication_rewriter = ConstBinaryExprRewriter {
            op: Operator::Multiply,
            f: Box::new(|left, right| {
                Ok(Transformed::yes(Expr::Literal(left.mul(right)?)))
            }),
        };
        // Create an expression from constant literals
        let expr = lit(6) + (lit(4) * (lit(2) + (lit(3) * lit(5))));
        // Run rewriters in a loop until constant expression is fully evaluated
        let (evaluated_expr, info) = RewriteCycle::new()
            .with_max_cycles(4)
            .each_cycle(expr, |cycle_state| {
                cycle_state
                    .rewrite(&mut addition_rewriter)?
                    .rewrite(&mut multiplication_rewriter)
            })
            .unwrap();
        assert_eq!(evaluated_expr, lit(74));
        assert_eq!(info.completed_cycles(), 3);
        assert_eq!(info.total_iterations(), 7);

        // Same expression as before
        let expr = lit(6) + (lit(4) * (lit(2) + (lit(3) * lit(5))));
        // Use `with_max_cycles` to end rewriting earlier
        let (evaluated_expr, info) = RewriteCycle::new()
            .with_max_cycles(2)
            .each_cycle(expr, |cycle_state| {
                cycle_state
                    .rewrite(&mut addition_rewriter)?
                    .rewrite(&mut multiplication_rewriter)
            })
            .unwrap();
        assert_eq!(evaluated_expr, lit(6) + lit(68));
        assert_eq!(info.completed_cycles(), 2);
        assert_eq!(info.total_iterations(), 4);
    }
}
