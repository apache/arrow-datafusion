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

use crate::expressions::Column;
use arrow_schema::SchemaRef;
use datafusion_common::{JoinSide, JoinType};
use indexmap::IndexSet;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::equivalence::{
    collapse_lex_req, EquivalenceGroup, OrderingEquivalenceClass, ProjectionMapping,
};

use crate::expressions::Literal;
use crate::sort_properties::{ExprOrdering, SortProperties};
use crate::{
    physical_exprs_contains, LexOrdering, LexOrderingRef, LexRequirement,
    LexRequirementRef, PhysicalExpr, PhysicalSortExpr, PhysicalSortRequirement,
};
use datafusion_common::tree_node::{Transformed, TreeNode};

use super::ordering::collapse_lex_ordering;

/// A `EquivalenceProperties` object stores useful information related to a schema.
/// Currently, it keeps track of:
/// - Equivalent expressions, e.g expressions that have same value.
/// - Valid sort expressions (orderings) for the schema.
/// - Constants expressions (e.g expressions that are known to have constant values).
///
/// Consider table below:
///
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 9 |
/// | 2 | 8 |
/// | 3 | 7 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
///
/// where both `a ASC` and `b DESC` can describe the table ordering. With
/// `EquivalenceProperties`, we can keep track of these different valid sort
/// expressions and treat `a ASC` and `b DESC` on an equal footing.
///
/// Similarly, consider the table below:
///
/// ```text
/// ┌-------┐
/// | a | b |
/// |---|---|
/// | 1 | 1 |
/// | 2 | 2 |
/// | 3 | 3 |
/// | 5 | 5 |
/// └---┴---┘
/// ```
///
/// where columns `a` and `b` always have the same value. We keep track of such
/// equivalences inside this object. With this information, we can optimize
/// things like partitioning. For example, if the partition requirement is
/// `Hash(a)` and output partitioning is `Hash(b)`, then we can deduce that
/// the existing partitioning satisfies the requirement.
#[derive(Debug, Clone)]
pub struct EquivalenceProperties {
    /// Collection of equivalence classes that store expressions with the same
    /// value.
    pub eq_group: EquivalenceGroup,
    /// Equivalent sort expressions for this table.
    pub oeq_class: OrderingEquivalenceClass,
    /// Expressions whose values are constant throughout the table.
    /// TODO: We do not need to track constants separately, they can be tracked
    ///       inside `eq_groups` as `Literal` expressions.
    pub constants: Vec<Arc<dyn PhysicalExpr>>,
    /// Schema associated with this object.
    schema: SchemaRef,
}

impl EquivalenceProperties {
    /// Creates an empty `EquivalenceProperties` object.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            eq_group: EquivalenceGroup::empty(),
            oeq_class: OrderingEquivalenceClass::empty(),
            constants: vec![],
            schema,
        }
    }

    /// Creates a new `EquivalenceProperties` object with the given orderings.
    pub fn new_with_orderings(schema: SchemaRef, orderings: &[LexOrdering]) -> Self {
        Self {
            eq_group: EquivalenceGroup::empty(),
            oeq_class: OrderingEquivalenceClass::new(orderings.to_vec()),
            constants: vec![],
            schema,
        }
    }

    /// Returns the associated schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Returns a reference to the ordering equivalence class within.
    pub fn oeq_class(&self) -> &OrderingEquivalenceClass {
        &self.oeq_class
    }

    /// Returns a reference to the equivalence group within.
    pub fn eq_group(&self) -> &EquivalenceGroup {
        &self.eq_group
    }

    /// Returns a reference to the constant expressions
    pub fn constants(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.constants
    }

    /// Returns the normalized version of the ordering equivalence class within.
    /// Normalization removes constants and duplicates as well as standardizing
    /// expressions according to the equivalence group within.
    pub fn normalized_oeq_class(&self) -> OrderingEquivalenceClass {
        OrderingEquivalenceClass::new(
            self.oeq_class
                .iter()
                .map(|ordering| self.normalize_sort_exprs(ordering))
                .collect(),
        )
    }

    /// Extends this `EquivalenceProperties` with the `other` object.
    pub fn extend(mut self, other: Self) -> Self {
        self.eq_group.extend(other.eq_group);
        self.oeq_class.extend(other.oeq_class);
        self.add_constants(other.constants)
    }

    /// Clears (empties) the ordering equivalence class within this object.
    /// Call this method when existing orderings are invalidated.
    pub fn clear_orderings(&mut self) {
        self.oeq_class.clear();
    }

    /// Extends this `EquivalenceProperties` by adding the orderings inside the
    /// ordering equivalence class `other`.
    pub fn add_ordering_equivalence_class(&mut self, other: OrderingEquivalenceClass) {
        self.oeq_class.extend(other);
    }

    /// Adds new orderings into the existing ordering equivalence class.
    pub fn add_new_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = LexOrdering>,
    ) {
        self.oeq_class.add_new_orderings(orderings);
    }

    /// Incorporates the given equivalence group to into the existing
    /// equivalence group within.
    pub fn add_equivalence_group(&mut self, other_eq_group: EquivalenceGroup) {
        self.eq_group.extend(other_eq_group);
    }

    /// Adds a new equality condition into the existing equivalence group.
    /// If the given equality defines a new equivalence class, adds this new
    /// equivalence class to the equivalence group.
    pub fn add_equal_conditions(
        &mut self,
        left: &Arc<dyn PhysicalExpr>,
        right: &Arc<dyn PhysicalExpr>,
    ) {
        self.eq_group.add_equal_conditions(left, right);
    }

    /// Track/register physical expressions with constant values.
    pub fn add_constants(
        mut self,
        constants: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    ) -> Self {
        for expr in self.eq_group.normalize_exprs(constants) {
            if !physical_exprs_contains(&self.constants, &expr) {
                self.constants.push(expr);
            }
        }
        self
    }

    /// Updates the ordering equivalence group within assuming that the table
    /// is re-sorted according to the argument `sort_exprs`. Note that constants
    /// and equivalence classes are unchanged as they are unaffected by a re-sort.
    pub fn with_reorder(mut self, sort_exprs: Vec<PhysicalSortExpr>) -> Self {
        // TODO: In some cases, existing ordering equivalences may still be valid add this analysis.
        self.oeq_class = OrderingEquivalenceClass::new(vec![sort_exprs]);
        self
    }

    /// Normalizes the given sort expressions (i.e. `sort_exprs`) using the
    /// equivalence group and the ordering equivalence class within.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases.
    /// Also assume that `self.oeq_class` states orderings `d ASC` and `a ASC, c ASC`
    /// are equivalent (in the sense that both describe the ordering of the table).
    /// If the `sort_exprs` argument were `vec![b ASC, c ASC, a ASC]`, then this
    /// function would return `vec![a ASC, c ASC]`. Internally, it would first
    /// normalize to `vec![a ASC, c ASC, a ASC]` and end up with the final result
    /// after deduplication.
    fn normalize_sort_exprs(&self, sort_exprs: LexOrderingRef) -> LexOrdering {
        // Convert sort expressions to sort requirements:
        let sort_reqs = PhysicalSortRequirement::from_sort_exprs(sort_exprs.iter());
        // Normalize the requirements:
        let normalized_sort_reqs = self.normalize_sort_requirements(&sort_reqs);
        // Convert sort requirements back to sort expressions:
        PhysicalSortRequirement::to_sort_exprs(normalized_sort_reqs)
    }

    /// Normalizes the given sort requirements (i.e. `sort_reqs`) using the
    /// equivalence group and the ordering equivalence class within. It works by:
    /// - Removing expressions that have a constant value from the given requirement.
    /// - Replacing sections that belong to some equivalence class in the equivalence
    ///   group with the first entry in the matching equivalence class.
    ///
    /// Assume that `self.eq_group` states column `a` and `b` are aliases.
    /// Also assume that `self.oeq_class` states orderings `d ASC` and `a ASC, c ASC`
    /// are equivalent (in the sense that both describe the ordering of the table).
    /// If the `sort_reqs` argument were `vec![b ASC, c ASC, a ASC]`, then this
    /// function would return `vec![a ASC, c ASC]`. Internally, it would first
    /// normalize to `vec![a ASC, c ASC, a ASC]` and end up with the final result
    /// after deduplication.
    fn normalize_sort_requirements(
        &self,
        sort_reqs: LexRequirementRef,
    ) -> LexRequirement {
        let normalized_sort_reqs = self.eq_group.normalize_sort_requirements(sort_reqs);
        let constants_normalized = self.eq_group.normalize_exprs(self.constants.clone());
        // Prune redundant sections in the requirement:
        collapse_lex_req(
            normalized_sort_reqs
                .iter()
                .filter(|&order| {
                    !physical_exprs_contains(&constants_normalized, &order.expr)
                })
                .cloned()
                .collect(),
        )
    }

    /// Checks whether the given ordering is satisfied by any of the existing
    /// orderings.
    pub fn ordering_satisfy(&self, given: LexOrderingRef) -> bool {
        // Convert the given sort expressions to sort requirements:
        let sort_requirements = PhysicalSortRequirement::from_sort_exprs(given.iter());
        self.ordering_satisfy_requirement(&sort_requirements)
    }

    /// Checks whether the given sort requirements are satisfied by any of the
    /// existing orderings.
    pub fn ordering_satisfy_requirement(&self, reqs: LexRequirementRef) -> bool {
        let mut eq_properties = self.clone();
        // First, standardize the given requirement:
        let normalized_reqs = eq_properties.normalize_sort_requirements(reqs);
        for normalized_req in normalized_reqs {
            // Check whether given ordering is satisfied
            if !eq_properties.ordering_satisfy_single(&normalized_req) {
                return false;
            }
            // Treat satisfied keys as constants in subsequent iterations. We
            // can do this because the "next" key only matters in a lexicographical
            // ordering when the keys to its left have the same values.
            //
            // Note that these expressions are not properly "constants". This is just
            // an implementation strategy confined to this function.
            //
            // For example, assume that the requirement is `[a ASC, (b + c) ASC]`,
            // and existing equivalent orderings are `[a ASC, b ASC]` and `[c ASC]`.
            // From the analysis above, we know that `[a ASC]` is satisfied. Then,
            // we add column `a` as constant to the algorithm state. This enables us
            // to deduce that `(b + c) ASC` is satisfied, given `a` is constant.
            eq_properties =
                eq_properties.add_constants(std::iter::once(normalized_req.expr));
        }
        true
    }

    /// Determines whether the ordering specified by the given sort requirement
    /// is satisfied based on the orderings within, equivalence classes, and
    /// constant expressions.
    ///
    /// # Arguments
    ///
    /// - `req`: A reference to a `PhysicalSortRequirement` for which the ordering
    ///   satisfaction check will be done.
    ///
    /// # Returns
    ///
    /// Returns `true` if the specified ordering is satisfied, `false` otherwise.
    fn ordering_satisfy_single(&self, req: &PhysicalSortRequirement) -> bool {
        let expr_ordering = self.get_expr_ordering(req.expr.clone());
        let ExprOrdering { expr, state, .. } = expr_ordering;
        match state {
            SortProperties::Ordered(options) => {
                let sort_expr = PhysicalSortExpr { expr, options };
                sort_expr.satisfy(req, self.schema())
            }
            // Singleton expressions satisfies any ordering.
            SortProperties::Singleton => true,
            SortProperties::Unordered => false,
        }
    }

    /// Checks whether the `given`` sort requirements are equal or more specific
    /// than the `reference` sort requirements.
    pub fn requirements_compatible(
        &self,
        given: LexRequirementRef,
        reference: LexRequirementRef,
    ) -> bool {
        let normalized_given = self.normalize_sort_requirements(given);
        let normalized_reference = self.normalize_sort_requirements(reference);

        (normalized_reference.len() <= normalized_given.len())
            && normalized_reference
                .into_iter()
                .zip(normalized_given)
                .all(|(reference, given)| given.compatible(&reference))
    }

    /// Returns the finer ordering among the orderings `lhs` and `rhs`, breaking
    /// any ties by choosing `lhs`.
    ///
    /// The finer ordering is the ordering that satisfies both of the orderings.
    /// If the orderings are incomparable, returns `None`.
    ///
    /// For example, the finer ordering among `[a ASC]` and `[a ASC, b ASC]` is
    /// the latter.
    pub fn get_finer_ordering(
        &self,
        lhs: LexOrderingRef,
        rhs: LexOrderingRef,
    ) -> Option<LexOrdering> {
        // Convert the given sort expressions to sort requirements:
        let lhs = PhysicalSortRequirement::from_sort_exprs(lhs);
        let rhs = PhysicalSortRequirement::from_sort_exprs(rhs);
        let finer = self.get_finer_requirement(&lhs, &rhs);
        // Convert the chosen sort requirements back to sort expressions:
        finer.map(PhysicalSortRequirement::to_sort_exprs)
    }

    /// Returns the finer ordering among the requirements `lhs` and `rhs`,
    /// breaking any ties by choosing `lhs`.
    ///
    /// The finer requirements are the ones that satisfy both of the given
    /// requirements. If the requirements are incomparable, returns `None`.
    ///
    /// For example, the finer requirements among `[a ASC]` and `[a ASC, b ASC]`
    /// is the latter.
    pub fn get_finer_requirement(
        &self,
        req1: LexRequirementRef,
        req2: LexRequirementRef,
    ) -> Option<LexRequirement> {
        let mut lhs = self.normalize_sort_requirements(req1);
        let mut rhs = self.normalize_sort_requirements(req2);
        lhs.iter_mut()
            .zip(rhs.iter_mut())
            .all(|(lhs, rhs)| {
                lhs.expr.eq(&rhs.expr)
                    && match (lhs.options, rhs.options) {
                        (Some(lhs_opt), Some(rhs_opt)) => lhs_opt == rhs_opt,
                        (Some(options), None) => {
                            rhs.options = Some(options);
                            true
                        }
                        (None, Some(options)) => {
                            lhs.options = Some(options);
                            true
                        }
                        (None, None) => true,
                    }
            })
            .then_some(if lhs.len() >= rhs.len() { lhs } else { rhs })
    }

    /// Calculates the "meet" of the given orderings (`lhs` and `rhs`).
    /// The meet of a set of orderings is the finest ordering that is satisfied
    /// by all the orderings in that set. For details, see:
    ///
    /// <https://en.wikipedia.org/wiki/Join_and_meet>
    ///
    /// If there is no ordering that satisfies both `lhs` and `rhs`, returns
    /// `None`. As an example, the meet of orderings `[a ASC]` and `[a ASC, b ASC]`
    /// is `[a ASC]`.
    pub fn get_meet_ordering(
        &self,
        lhs: LexOrderingRef,
        rhs: LexOrderingRef,
    ) -> Option<LexOrdering> {
        let lhs = self.normalize_sort_exprs(lhs);
        let rhs = self.normalize_sort_exprs(rhs);
        let mut meet = vec![];
        for (lhs, rhs) in lhs.into_iter().zip(rhs.into_iter()) {
            if lhs.eq(&rhs) {
                meet.push(lhs);
            } else {
                break;
            }
        }
        (!meet.is_empty()).then_some(meet)
    }

    /// Projects argument `expr` according to `projection_mapping`, taking
    /// equivalences into account.
    ///
    /// For example, assume that columns `a` and `c` are always equal, and that
    /// `projection_mapping` encodes following mapping:
    ///
    /// ```text
    /// a -> a1
    /// b -> b1
    /// ```
    ///
    /// Then, this function projects `a + b` to `Some(a1 + b1)`, `c + b` to
    /// `Some(a1 + b1)` and `d` to `None`, meaning that it  cannot be projected.
    pub fn project_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        projection_mapping: &ProjectionMapping,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        self.eq_group.project_expr(projection_mapping, expr)
    }

    /// Constructs a dependency map based on existing orderings referred to in
    /// the projection.
    ///
    /// This function analyzes the orderings in the normalized order-equivalence
    /// class and builds a dependency map. The dependency map captures relationships
    /// between expressions within the orderings, helping to identify dependencies
    /// and construct valid projected orderings during projection operations.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the `ProjectionMapping` that defines the
    ///   relationship between source and target expressions.
    ///
    /// # Returns
    ///
    /// A [`DependencyMap`] representing the dependency map, where each
    /// [`DependencyNode`] contains dependencies for the key [`PhysicalSortExpr`].
    ///
    /// # Example
    ///
    /// Assume we have two equivalent orderings: `[a ASC, b ASC]` and `[a ASC, c ASC]`,
    /// and the projection mapping is `[a -> a_new, b -> b_new, b + c -> b + c]`.
    /// Then, the dependency map will be:
    ///
    /// ```text
    /// a ASC: Node {Some(a_new ASC), HashSet{}}
    /// b ASC: Node {Some(b_new ASC), HashSet{a ASC}}
    /// c ASC: Node {None, HashSet{a ASC}}
    /// ```
    fn construct_dependency_map(&self, mapping: &ProjectionMapping) -> DependencyMap {
        let mut dependency_map = HashMap::new();
        for ordering in self.normalized_oeq_class().iter() {
            for (idx, sort_expr) in ordering.iter().enumerate() {
                let target_sort_expr =
                    self.project_expr(&sort_expr.expr, mapping).map(|expr| {
                        PhysicalSortExpr {
                            expr,
                            options: sort_expr.options,
                        }
                    });
                let is_projected = target_sort_expr.is_some();
                if is_projected
                    || mapping
                        .iter()
                        .any(|(source, _)| expr_refers(source, &sort_expr.expr))
                {
                    // Previous ordering is a dependency. Note that there is no,
                    // dependency for a leading ordering (i.e. the first sort
                    // expression).
                    let dependency = idx.checked_sub(1).map(|a| &ordering[a]);
                    // Add sort expressions that can be projected or referred to
                    // by any of the projection expressions to the dependency map:
                    dependency_map
                        .entry(sort_expr.clone())
                        .or_insert_with(|| DependencyNode {
                            target_sort_expr: target_sort_expr.clone(),
                            dependencies: HashSet::new(),
                        })
                        .insert_dependency(dependency);
                }
                if !is_projected {
                    // If we can not project, stop constructing the dependency
                    // map as remaining dependencies will be invalid after projection.
                    break;
                }
            }
        }
        dependency_map
    }

    /// Returns a new `ProjectionMapping` where source expressions are normalized.
    ///
    /// This normalization ensures that source expressions are transformed into a
    /// consistent representation. This is beneficial for algorithms that rely on
    /// exact equalities, as it allows for more precise and reliable comparisons.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the original `ProjectionMapping` to be normalized.
    ///
    /// # Returns
    ///
    /// A new `ProjectionMapping` with normalized source expressions.
    fn normalized_mapping(&self, mapping: &ProjectionMapping) -> ProjectionMapping {
        // Construct the mapping where source expressions are normalized. In this way
        // In the algorithms below we can work on exact equalities
        ProjectionMapping {
            map: mapping
                .iter()
                .map(|(source, target)| {
                    let normalized_source = self.eq_group.normalize_expr(source.clone());
                    (normalized_source, target.clone())
                })
                .collect(),
        }
    }

    /// Computes projected orderings based on a given projection mapping.
    ///
    /// This function takes a `ProjectionMapping` and computes the possible
    /// orderings for the projected expressions. It considers dependencies
    /// between expressions and generates valid orderings according to the
    /// specified sort properties.
    ///
    /// # Parameters
    ///
    /// - `mapping`: A reference to the `ProjectionMapping` that defines the
    ///   relationship between source and target expressions.
    ///
    /// # Returns
    ///
    /// A vector of `LexOrdering` containing all valid orderings after projection.
    fn projected_orderings(&self, mapping: &ProjectionMapping) -> Vec<LexOrdering> {
        let mapping = self.normalized_mapping(mapping);

        // Get dependency map for existing orderings:
        let dependency_map = self.construct_dependency_map(&mapping);

        let orderings = mapping.iter().flat_map(|(source, target)| {
            referred_dependencies(&dependency_map, source)
                .into_iter()
                .filter_map(|relevant_deps| {
                    if let SortProperties::Ordered(options) =
                        get_expr_ordering(source, &relevant_deps)
                    {
                        Some((options, relevant_deps))
                    } else {
                        // Do not consider unordered cases
                        None
                    }
                })
                .flat_map(|(options, relevant_deps)| {
                    let sort_expr = PhysicalSortExpr {
                        expr: target.clone(),
                        options,
                    };
                    // Generate dependent orderings (i.e. prefixes for `sort_expr`):
                    let mut dependency_orderings =
                        generate_dependency_orderings(&relevant_deps, &dependency_map);
                    // Append `sort_expr` to the dependent orderings:
                    for ordering in dependency_orderings.iter_mut() {
                        ordering.push(sort_expr.clone());
                    }
                    dependency_orderings
                })
        });

        // Add valid projected orderings. For example, if existing ordering is
        // `a + b` and projection is `[a -> a_new, b -> b_new]`, we need to
        // preserve `a_new + b_new` as ordered. Please note that `a_new` and
        // `b_new` themselves need not be ordered. Such dependencies cannot be
        // deduced via the pass above.
        let projected_orderings = dependency_map.iter().flat_map(|(sort_expr, node)| {
            let mut prefixes = construct_prefix_orderings(sort_expr, &dependency_map);
            if prefixes.is_empty() {
                // If prefix is empty, there is no dependency. Insert
                // empty ordering:
                prefixes = vec![vec![]];
            }
            // Append current ordering on top its dependencies:
            for ordering in prefixes.iter_mut() {
                if let Some(target) = &node.target_sort_expr {
                    ordering.push(target.clone())
                }
            }
            prefixes
        });

        // Simplify each ordering by removing redundant sections:
        orderings
            .chain(projected_orderings)
            .map(collapse_lex_ordering)
            .collect()
    }

    /// Projects constants based on the provided `ProjectionMapping`.
    ///
    /// This function takes a `ProjectionMapping` and identifies/projects
    /// constants based on the existing constants and the mapping. It ensures
    /// that constants are appropriately propagated through the projection.
    ///
    /// # Arguments
    ///
    /// - `mapping`: A reference to a `ProjectionMapping` representing the
    ///   mapping of source expressions to target expressions in the projection.
    ///
    /// # Returns
    ///
    /// Returns a `Vec<Arc<dyn PhysicalExpr>>` containing the projected constants.
    fn projected_constants(
        &self,
        mapping: &ProjectionMapping,
    ) -> Vec<Arc<dyn PhysicalExpr>> {
        // First, project existing constants. For example, assume that `a + b`
        // is known to be constant. If the projection were `a as a_new`, `b as b_new`,
        // then we would project constant `a + b` as `a_new + b_new`.
        let mut projected_constants = self
            .constants
            .iter()
            .flat_map(|expr| self.eq_group.project_expr(mapping, expr))
            .collect::<Vec<_>>();
        // Add projection expressions that are known to be constant:
        for (source, target) in mapping.iter() {
            if self.is_expr_constant(source)
                && !physical_exprs_contains(&projected_constants, target)
            {
                projected_constants.push(target.clone());
            }
        }
        projected_constants
    }

    /// Projects the equivalences within according to `projection_mapping`
    /// and `output_schema`.
    pub fn project(
        &self,
        projection_mapping: &ProjectionMapping,
        output_schema: SchemaRef,
    ) -> Self {
        let projected_constants = self.projected_constants(projection_mapping);
        let projected_eq_group = self.eq_group.project(projection_mapping);
        let projected_orderings = self.projected_orderings(projection_mapping);
        Self {
            eq_group: projected_eq_group,
            oeq_class: OrderingEquivalenceClass::new(projected_orderings),
            constants: projected_constants,
            schema: output_schema,
        }
    }

    /// Returns the longest (potentially partial) permutation satisfying the
    /// existing ordering. For example, if we have the equivalent orderings
    /// `[a ASC, b ASC]` and `[c DESC]`, with `exprs` containing `[c, b, a, d]`,
    /// then this function returns `([a ASC, b ASC, c DESC], [2, 1, 0])`.
    /// This means that the specification `[a ASC, b ASC, c DESC]` is satisfied
    /// by the existing ordering, and `[a, b, c]` resides at indices: `2, 1, 0`
    /// inside the argument `exprs` (respectively). For the mathematical
    /// definition of "partial permutation", see:
    ///
    /// <https://en.wikipedia.org/wiki/Permutation#k-permutations_of_n>
    pub fn find_longest_permutation(
        &self,
        exprs: &[Arc<dyn PhysicalExpr>],
    ) -> (LexOrdering, Vec<usize>) {
        let mut eq_properties = self.clone();
        let mut result = vec![];
        // The algorithm is as follows:
        // - Iterate over all the expressions and insert ordered expressions
        //   into the result.
        // - Treat inserted expressions as constants (i.e. add them as constants
        //   to the state).
        // - Continue the above procedure until no expression is inserted; i.e.
        //   the algorithm reaches a fixed point.
        // This algorithm should reach a fixed point in at most `exprs.len()`
        // iterations.
        let mut search_indices = (0..exprs.len()).collect::<IndexSet<_>>();
        for _idx in 0..exprs.len() {
            // Get ordered expressions with their indices.
            let ordered_exprs = search_indices
                .iter()
                .flat_map(|&idx| {
                    let ExprOrdering { expr, state, .. } =
                        eq_properties.get_expr_ordering(exprs[idx].clone());
                    if let SortProperties::Ordered(options) = state {
                        Some((PhysicalSortExpr { expr, options }, idx))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            // We reached a fixed point, exit.
            if ordered_exprs.is_empty() {
                break;
            }
            // Remove indices that have an ordering from `search_indices`, and
            // treat ordered expressions as constants in subsequent iterations.
            // We can do this because the "next" key only matters in a lexicographical
            // ordering when the keys to its left have the same values.
            //
            // Note that these expressions are not properly "constants". This is just
            // an implementation strategy confined to this function.
            for (PhysicalSortExpr { expr, .. }, idx) in &ordered_exprs {
                eq_properties =
                    eq_properties.add_constants(std::iter::once(expr.clone()));
                search_indices.remove(idx);
            }
            // Add new ordered section to the state.
            result.extend(ordered_exprs);
        }
        result.into_iter().unzip()
    }

    /// This function determines whether the provided expression is constant
    /// based on the known constants.
    ///
    /// # Arguments
    ///
    /// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the
    ///   expression to be checked.
    ///
    /// # Returns
    ///
    /// Returns `true` if the expression is constant according to equivalence
    /// group, `false` otherwise.
    fn is_expr_constant(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        // As an example, assume that we know columns `a` and `b` are constant.
        // Then, `a`, `b` and `a + b` will all return `true` whereas `c` will
        // return `false`.
        let normalized_constants = self.eq_group.normalize_exprs(self.constants.to_vec());
        let normalized_expr = self.eq_group.normalize_expr(expr.clone());
        is_constant_recurse(&normalized_constants, &normalized_expr)
    }

    /// Retrieves the ordering information for a given physical expression.
    ///
    /// This function constructs an `ExprOrdering` object for the provided
    /// expression, which encapsulates information about the expression's
    /// ordering, including its [`SortProperties`].
    ///
    /// # Arguments
    ///
    /// - `expr`: An `Arc<dyn PhysicalExpr>` representing the physical expression
    ///   for which ordering information is sought.
    ///
    /// # Returns
    ///
    /// Returns an `ExprOrdering` object containing the ordering information for
    /// the given expression.
    pub fn get_expr_ordering(&self, expr: Arc<dyn PhysicalExpr>) -> ExprOrdering {
        ExprOrdering::new(expr.clone())
            .transform_up_old(&|expr| Ok(update_ordering(expr, self)))
            // Guaranteed to always return `Ok`.
            .unwrap()
    }
}

/// Calculates the [`SortProperties`] of a given [`ExprOrdering`] node.
/// The node can either be a leaf node, or an intermediate node:
/// - If it is a leaf node, we directly find the order of the node by looking
/// at the given sort expression and equivalence properties if it is a `Column`
/// leaf, or we mark it as unordered. In the case of a `Literal` leaf, we mark
/// it as singleton so that it can cooperate with all ordered columns.
/// - If it is an intermediate node, the children states matter. Each `PhysicalExpr`
/// and operator has its own rules on how to propagate the children orderings.
/// However, before we engage in recursion, we check whether this intermediate
/// node directly matches with the sort expression. If there is a match, the
/// sort expression emerges at that node immediately, discarding the recursive
/// result coming from its children.
fn update_ordering(
    mut node: ExprOrdering,
    eq_properties: &EquivalenceProperties,
) -> Transformed<ExprOrdering> {
    // We have a Column, which is one of the two possible leaf node types:
    let normalized_expr = eq_properties.eq_group.normalize_expr(node.expr.clone());
    if eq_properties.is_expr_constant(&normalized_expr) {
        node.state = SortProperties::Singleton;
    } else if let Some(options) = eq_properties
        .normalized_oeq_class()
        .get_options(&normalized_expr)
    {
        node.state = SortProperties::Ordered(options);
    } else if !node.expr.children().is_empty() {
        // We have an intermediate (non-leaf) node, account for its children:
        node.state = node.expr.get_ordering(&node.children_state());
    } else if node.expr.as_any().is::<Literal>() {
        // We have a Literal, which is the other possible leaf node type:
        node.state = node.expr.get_ordering(&[]);
    } else {
        return Transformed::No(node);
    }
    Transformed::Yes(node)
}

/// This function determines whether the provided expression is constant
/// based on the known constants.
///
/// # Arguments
///
/// - `constants`: A `&[Arc<dyn PhysicalExpr>]` containing expressions known to
///   be a constant.
/// - `expr`: A reference to a `Arc<dyn PhysicalExpr>` representing the expression
///   to check.
///
/// # Returns
///
/// Returns `true` if the expression is constant according to equivalence
/// group, `false` otherwise.
fn is_constant_recurse(
    constants: &[Arc<dyn PhysicalExpr>],
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    if physical_exprs_contains(constants, expr) {
        return true;
    }
    let children = expr.children();
    !children.is_empty() && children.iter().all(|c| is_constant_recurse(constants, c))
}

/// This function examines whether a referring expression directly refers to a
/// given referred expression or if any of its children in the expression tree
/// refer to the specified expression.
///
/// # Parameters
///
/// - `referring_expr`: A reference to the referring expression (`Arc<dyn PhysicalExpr>`).
/// - `referred_expr`: A reference to the referred expression (`Arc<dyn PhysicalExpr>`)
///
/// # Returns
///
/// A boolean value indicating whether `referring_expr` refers (needs it to evaluate its result)
/// `referred_expr` or not.
fn expr_refers(
    referring_expr: &Arc<dyn PhysicalExpr>,
    referred_expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    referring_expr.eq(referred_expr)
        || referring_expr
            .children()
            .iter()
            .any(|child| expr_refers(child, referred_expr))
}

/// This function analyzes the dependency map to collect referred dependencies for
/// a given source expression.
///
/// # Parameters
///
/// - `dependency_map`: A reference to the `DependencyMap` where each
///   `PhysicalSortExpr` is associated with a `DependencyNode`.
/// - `source`: A reference to the source expression (`Arc<dyn PhysicalExpr>`)
///   for which relevant dependencies need to be identified.
///
/// # Returns
///
/// A `Vec<Dependencies>` containing the dependencies for the given source
/// expression. These dependencies are expressions that are referred to by
/// the source expression based on the provided dependency map.
fn referred_dependencies(
    dependency_map: &DependencyMap,
    source: &Arc<dyn PhysicalExpr>,
) -> Vec<Dependencies> {
    // Associate `PhysicalExpr`s with `PhysicalSortExpr`s that contain them:
    let mut expr_to_sort_exprs = HashMap::<ExprWrapper, Dependencies>::new();
    for sort_expr in dependency_map
        .keys()
        .filter(|sort_expr| expr_refers(source, &sort_expr.expr))
    {
        let key = ExprWrapper(sort_expr.expr.clone());
        expr_to_sort_exprs
            .entry(key)
            .or_default()
            .insert(sort_expr.clone());
    }

    // Generate all valid dependencies for the source. For example, if the source
    // is `a + b` and the map is `[a -> (a ASC, a DESC), b -> (b ASC)]`, we get
    // `vec![HashSet(a ASC, b ASC), HashSet(a DESC, b ASC)]`.
    expr_to_sort_exprs
        .values()
        .multi_cartesian_product()
        .map(|referred_deps| referred_deps.into_iter().cloned().collect())
        .collect()
}

/// This function retrieves the dependencies of the given relevant sort expression
/// from the given dependency map. It then constructs prefix orderings by recursively
/// analyzing the dependencies and include them in the orderings.
///
/// # Parameters
///
/// - `relevant_sort_expr`: A reference to the relevant sort expression
///   (`PhysicalSortExpr`) for which prefix orderings are to be constructed.
/// - `dependency_map`: A reference to the `DependencyMap` containing dependencies.
///
/// # Returns
///
/// A vector of prefix orderings (`Vec<LexOrdering>`) based on the given relevant
/// sort expression and its dependencies.
fn construct_prefix_orderings(
    relevant_sort_expr: &PhysicalSortExpr,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    dependency_map[relevant_sort_expr]
        .dependencies
        .iter()
        .flat_map(|dep| construct_orderings(dep, dependency_map))
        .collect()
}

/// Given a set of relevant dependencies (`relevant_deps`) and a map of dependencies
/// (`dependency_map`), this function generates all possible prefix orderings
/// based on the given dependencies.
///
/// # Parameters
///
/// * `dependencies` - A reference to the dependencies.
/// * `dependency_map` - A reference to the map of dependencies for expressions.
///
/// # Returns
///
/// A vector of lexical orderings (`Vec<LexOrdering>`) representing all valid orderings
/// based on the given dependencies.
fn generate_dependency_orderings(
    dependencies: &Dependencies,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    // Construct all the valid prefix orderings for each expression appearing
    // in the projection:
    let relevant_prefixes = dependencies
        .iter()
        .flat_map(|dep| {
            let prefixes = construct_prefix_orderings(dep, dependency_map);
            (!prefixes.is_empty()).then_some(prefixes)
        })
        .collect::<Vec<_>>();

    // No dependency, dependent is a leading ordering.
    if relevant_prefixes.is_empty() {
        // Return an empty ordering:
        return vec![vec![]];
    }

    // Generate all possible orderings where dependencies are satisfied for the
    // current projection expression. For example, if expression is `a + b ASC`,
    // and the dependency for `a ASC` is `[c ASC]`, the dependency for `b ASC`
    // is `[d DESC]`, then we generate `[c ASC, d DESC, a + b ASC]` and
    // `[d DESC, c ASC, a + b ASC]`.
    relevant_prefixes
        .into_iter()
        .multi_cartesian_product()
        .flat_map(|prefix_orderings| {
            prefix_orderings
                .iter()
                .permutations(prefix_orderings.len())
                .map(|prefixes| prefixes.into_iter().flatten().cloned().collect())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// This function examines the given expression and the sort expressions it
/// refers to determine the ordering properties of the expression.
///
/// # Parameters
///
/// - `expr`: A reference to the source expression (`Arc<dyn PhysicalExpr>`) for
///   which ordering properties need to be determined.
/// - `dependencies`: A reference to `Dependencies`, containing sort expressions
///   referred to by `expr`.
///
/// # Returns
///
/// A `SortProperties` indicating the ordering information of the given expression.
fn get_expr_ordering(
    expr: &Arc<dyn PhysicalExpr>,
    dependencies: &Dependencies,
) -> SortProperties {
    if let Some(column_order) = dependencies.iter().find(|&order| expr.eq(&order.expr)) {
        // If exact match is found, return its ordering.
        SortProperties::Ordered(column_order.options)
    } else {
        // Find orderings of its children
        let child_states = expr
            .children()
            .iter()
            .map(|child| get_expr_ordering(child, dependencies))
            .collect::<Vec<_>>();
        // Calculate expression ordering using ordering of its children.
        expr.get_ordering(&child_states)
    }
}

/// Represents a node in the dependency map used to construct projected orderings.
///
/// A `DependencyNode` contains information about a particular sort expression,
/// including its target sort expression and a set of dependencies on other sort
/// expressions.
///
/// # Fields
///
/// - `target_sort_expr`: An optional `PhysicalSortExpr` representing the target
///   sort expression associated with the node. It is `None` if the sort expression
///   cannot be projected.
/// - `dependencies`: A [`Dependencies`] containing dependencies on other sort
///   expressions that are referred to by the target sort expression.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyNode {
    target_sort_expr: Option<PhysicalSortExpr>,
    dependencies: Dependencies,
}

impl DependencyNode {
    // Insert dependency to the state (if exists).
    fn insert_dependency(&mut self, dependency: Option<&PhysicalSortExpr>) {
        if let Some(dep) = dependency {
            self.dependencies.insert(dep.clone());
        }
    }
}

type DependencyMap = HashMap<PhysicalSortExpr, DependencyNode>;
type Dependencies = HashSet<PhysicalSortExpr>;

/// This function recursively analyzes the dependencies of the given sort
/// expression within the given dependency map to construct lexicographical
/// orderings that include the sort expression and its dependencies.
///
/// # Parameters
///
/// - `referred_sort_expr`: A reference to the sort expression (`PhysicalSortExpr`)
///   for which lexicographical orderings satisfying its dependencies are to be
///   constructed.
/// - `dependency_map`: A reference to the `DependencyMap` that contains
///   dependencies for different `PhysicalSortExpr`s.
///
/// # Returns
///
/// A vector of lexicographical orderings (`Vec<LexOrdering>`) based on the given
/// sort expression and its dependencies.
fn construct_orderings(
    referred_sort_expr: &PhysicalSortExpr,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    // We are sure that `referred_sort_expr` is inside `dependency_map`.
    let node = &dependency_map[referred_sort_expr];
    // Since we work on intermediate nodes, we are sure `val.target_sort_expr`
    // exists.
    let target_sort_expr = node.target_sort_expr.clone().unwrap();
    if node.dependencies.is_empty() {
        vec![vec![target_sort_expr]]
    } else {
        node.dependencies
            .iter()
            .flat_map(|dep| {
                let mut orderings = construct_orderings(dep, dependency_map);
                for ordering in orderings.iter_mut() {
                    ordering.push(target_sort_expr.clone())
                }
                orderings
            })
            .collect()
    }
}

/// Calculate ordering equivalence properties for the given join operation.
pub fn join_equivalence_properties(
    left: EquivalenceProperties,
    right: EquivalenceProperties,
    join_type: &JoinType,
    join_schema: SchemaRef,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
    on: &[(Column, Column)],
) -> EquivalenceProperties {
    let left_size = left.schema.fields.len();
    let mut result = EquivalenceProperties::new(join_schema);
    result.add_equivalence_group(left.eq_group().join(
        right.eq_group(),
        join_type,
        left_size,
        on,
    ));

    let left_oeq_class = left.oeq_class;
    let mut right_oeq_class = right.oeq_class;
    match maintains_input_order {
        [true, false] => {
            // In this special case, right side ordering can be prefixed with
            // the left side ordering.
            if let (Some(JoinSide::Left), JoinType::Inner) = (probe_side, join_type) {
                updated_right_ordering_equivalence_class(
                    &mut right_oeq_class,
                    join_type,
                    left_size,
                );

                // Right side ordering equivalence properties should be prepended
                // with those of the left side while constructing output ordering
                // equivalence properties since stream side is the left side.
                //
                // For example, if the right side ordering equivalences contain
                // `b ASC`, and the left side ordering equivalences contain `a ASC`,
                // then we should add `a ASC, b ASC` to the ordering equivalences
                // of the join output.
                let out_oeq_class = left_oeq_class.join_suffix(&right_oeq_class);
                result.add_ordering_equivalence_class(out_oeq_class);
            } else {
                result.add_ordering_equivalence_class(left_oeq_class);
            }
        }
        [false, true] => {
            updated_right_ordering_equivalence_class(
                &mut right_oeq_class,
                join_type,
                left_size,
            );
            // In this special case, left side ordering can be prefixed with
            // the right side ordering.
            if let (Some(JoinSide::Right), JoinType::Inner) = (probe_side, join_type) {
                // Left side ordering equivalence properties should be prepended
                // with those of the right side while constructing output ordering
                // equivalence properties since stream side is the right side.
                //
                // For example, if the left side ordering equivalences contain
                // `a ASC`, and the right side ordering equivalences contain `b ASC`,
                // then we should add `b ASC, a ASC` to the ordering equivalences
                // of the join output.
                let out_oeq_class = right_oeq_class.join_suffix(&left_oeq_class);
                result.add_ordering_equivalence_class(out_oeq_class);
            } else {
                result.add_ordering_equivalence_class(right_oeq_class);
            }
        }
        [false, false] => {}
        [true, true] => unreachable!("Cannot maintain ordering of both sides"),
        _ => unreachable!("Join operators can not have more than two children"),
    }
    result
}

/// In the context of a join, update the right side `OrderingEquivalenceClass`
/// so that they point to valid indices in the join output schema.
///
/// To do so, we increment column indices by the size of the left table when
/// join schema consists of a combination of the left and right schemas. This
/// is the case for `Inner`, `Left`, `Full` and `Right` joins. For other cases,
/// indices do not change.
fn updated_right_ordering_equivalence_class(
    right_oeq_class: &mut OrderingEquivalenceClass,
    join_type: &JoinType,
    left_size: usize,
) {
    if matches!(
        join_type,
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right
    ) {
        right_oeq_class.add_offset(left_size);
    }
}

/// Wrapper struct for `Arc<dyn PhysicalExpr>` to use them as keys in a hash map.
#[derive(Debug, Clone)]
struct ExprWrapper(Arc<dyn PhysicalExpr>);

impl PartialEq<Self> for ExprWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for ExprWrapper {}

impl Hash for ExprWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::equivalence::add_offset_to_expr;
    use crate::equivalence::tests::{
        convert_to_orderings, convert_to_sort_exprs, convert_to_sort_reqs,
        create_random_schema, create_test_params, create_test_schema,
        generate_table_for_eq_properties, is_table_same_after_sort, output_schema,
    };
    use crate::execution_props::ExecutionProps;
    use crate::expressions::{col, BinaryExpr, Column};
    use crate::functions::create_physical_expr;
    use crate::PhysicalSortExpr;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::{Fields, SortOptions, TimeUnit};
    use datafusion_common::Result;
    use datafusion_expr::{BuiltinScalarFunction, Operator};
    use itertools::Itertools;

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let input_properties = EquivalenceProperties::new(input_schema.clone());
        let col_a = col("a", &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (col_a.clone(), "a1".to_string()),
            (col_a.clone(), "a2".to_string()),
            (col_a.clone(), "a3".to_string()),
            (col_a.clone(), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        let out_schema = output_schema(&projection_mapping, &input_schema)?;
        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (col_a.clone(), "a1".to_string()),
            (col_a.clone(), "a2".to_string()),
            (col_a.clone(), "a3".to_string()),
            (col_a.clone(), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let col_a1 = &col("a1", &out_schema)?;
        let col_a2 = &col("a2", &out_schema)?;
        let col_a3 = &col("a3", &out_schema)?;
        let col_a4 = &col("a4", &out_schema)?;
        let out_properties = input_properties.project(&projection_mapping, out_schema);

        // At the output a1=a2=a3=a4
        assert_eq!(out_properties.eq_group().len(), 1);
        let eq_class = &out_properties.eq_group().classes[0];
        assert_eq!(eq_class.len(), 4);
        assert!(eq_class.contains(col_a1));
        assert!(eq_class.contains(col_a2));
        assert!(eq_class.contains(col_a3));
        assert!(eq_class.contains(col_a4));

        Ok(())
    }

    #[test]
    fn test_join_equivalence_properties() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let offset = schema.fields.len();
        let col_a2 = &add_offset_to_expr(col_a.clone(), offset);
        let col_b2 = &add_offset_to_expr(col_b.clone(), offset);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let test_cases = vec![
            // ------- TEST CASE 1 --------
            // [a ASC], [b ASC]
            (
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // expected [a ASC, a2 ASC], [a ASC, b2 ASC], [b ASC, a2 ASC], [b ASC, b2 ASC]
                vec![
                    vec![(col_a, option_asc), (col_a2, option_asc)],
                    vec![(col_a, option_asc), (col_b2, option_asc)],
                    vec![(col_b, option_asc), (col_a2, option_asc)],
                    vec![(col_b, option_asc), (col_b2, option_asc)],
                ],
            ),
            // ------- TEST CASE 2 --------
            // [a ASC], [b ASC]
            (
                // [a ASC], [b ASC], [c ASC]
                vec![
                    vec![(col_a, option_asc)],
                    vec![(col_b, option_asc)],
                    vec![(col_c, option_asc)],
                ],
                // [a ASC], [b ASC]
                vec![vec![(col_a, option_asc)], vec![(col_b, option_asc)]],
                // expected [a ASC, a2 ASC], [a ASC, b2 ASC], [b ASC, a2 ASC], [b ASC, b2 ASC], [c ASC, a2 ASC], [c ASC, b2 ASC]
                vec![
                    vec![(col_a, option_asc), (col_a2, option_asc)],
                    vec![(col_a, option_asc), (col_b2, option_asc)],
                    vec![(col_b, option_asc), (col_a2, option_asc)],
                    vec![(col_b, option_asc), (col_b2, option_asc)],
                    vec![(col_c, option_asc), (col_a2, option_asc)],
                    vec![(col_c, option_asc), (col_b2, option_asc)],
                ],
            ),
        ];
        for (left_orderings, right_orderings, expected) in test_cases {
            let mut left_eq_properties = EquivalenceProperties::new(schema.clone());
            let mut right_eq_properties = EquivalenceProperties::new(schema.clone());
            let left_orderings = convert_to_orderings(&left_orderings);
            let right_orderings = convert_to_orderings(&right_orderings);
            let expected = convert_to_orderings(&expected);
            left_eq_properties.add_new_orderings(left_orderings);
            right_eq_properties.add_new_orderings(right_orderings);
            let join_eq = join_equivalence_properties(
                left_eq_properties,
                right_eq_properties,
                &JoinType::Inner,
                Arc::new(Schema::empty()),
                &[true, false],
                Some(JoinSide::Left),
                &[],
            );
            let orderings = &join_eq.oeq_class.orderings;
            let err_msg = format!("expected: {:?}, actual:{:?}", expected, orderings);
            assert_eq!(
                join_eq.oeq_class.orderings.len(),
                expected.len(),
                "{}",
                err_msg
            );
            for ordering in orderings {
                assert!(
                    expected.contains(ordering),
                    "{}, ordering: {:?}",
                    err_msg,
                    ordering
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_expr_consists_of_constants() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_d = col("d", &schema)?;
        let b_plus_d = Arc::new(BinaryExpr::new(
            col_b.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let constants = vec![col_a.clone(), col_b.clone()];
        let expr = b_plus_d.clone();
        assert!(!is_constant_recurse(&constants, &expr));

        let constants = vec![col_a.clone(), col_b.clone(), col_d.clone()];
        let expr = b_plus_d.clone();
        assert!(is_constant_recurse(&constants, &expr));
        Ok(())
    }

    #[test]
    fn test_get_updated_right_ordering_equivalence_properties() -> Result<()> {
        let join_type = JoinType::Inner;
        // Join right child schema
        let child_fields: Fields = ["x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();
        let child_schema = Schema::new(child_fields);
        let col_x = &col("x", &child_schema)?;
        let col_y = &col("y", &child_schema)?;
        let col_z = &col("z", &child_schema)?;
        let col_w = &col("w", &child_schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // [x ASC, y ASC], [z ASC, w ASC]
        let orderings = vec![
            vec![(col_x, option_asc), (col_y, option_asc)],
            vec![(col_z, option_asc), (col_w, option_asc)],
        ];
        let orderings = convert_to_orderings(&orderings);
        // Right child ordering equivalences
        let mut right_oeq_class = OrderingEquivalenceClass::new(orderings);

        let left_columns_len = 4;

        let fields: Fields = ["a", "b", "c", "d", "x", "y", "z", "w"]
            .into_iter()
            .map(|name| Field::new(name, DataType::Int32, true))
            .collect();

        // Join Schema
        let schema = Schema::new(fields);
        let col_a = &col("a", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_x = &col("x", &schema)?;
        let col_y = &col("y", &schema)?;
        let col_z = &col("z", &schema)?;
        let col_w = &col("w", &schema)?;

        let mut join_eq_properties = EquivalenceProperties::new(Arc::new(schema));
        // a=x and d=w
        join_eq_properties.add_equal_conditions(col_a, col_x);
        join_eq_properties.add_equal_conditions(col_d, col_w);

        updated_right_ordering_equivalence_class(
            &mut right_oeq_class,
            &join_type,
            left_columns_len,
        );
        join_eq_properties.add_ordering_equivalence_class(right_oeq_class);
        let result = join_eq_properties.oeq_class().clone();

        // [x ASC, y ASC], [z ASC, w ASC]
        let orderings = vec![
            vec![(col_x, option_asc), (col_y, option_asc)],
            vec![(col_z, option_asc), (col_w, option_asc)],
        ];
        let orderings = convert_to_orderings(&orderings);
        let expected = OrderingEquivalenceClass::new(orderings);

        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn test_normalize_ordering_equivalence_classes() -> Result<()> {
        let sort_options = SortOptions::default();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a_expr = col("a", &schema)?;
        let col_b_expr = col("b", &schema)?;
        let col_c_expr = col("c", &schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));

        eq_properties.add_equal_conditions(&col_a_expr, &col_c_expr);
        let others = vec![
            vec![PhysicalSortExpr {
                expr: col_b_expr.clone(),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: col_c_expr.clone(),
                options: sort_options,
            }],
        ];
        eq_properties.add_new_orderings(others);

        let mut expected_eqs = EquivalenceProperties::new(Arc::new(schema));
        expected_eqs.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: col_b_expr.clone(),
                options: sort_options,
            }],
            vec![PhysicalSortExpr {
                expr: col_c_expr.clone(),
                options: sort_options,
            }],
        ]);

        let oeq_class = eq_properties.oeq_class().clone();
        let expected = expected_eqs.oeq_class();
        assert!(oeq_class.eq(expected));

        Ok(())
    }

    #[test]
    fn test_get_indices_of_matching_sort_exprs_with_order_eq() -> Result<()> {
        let sort_options = SortOptions::default();
        let sort_options_not = SortOptions::default().not();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
            ]
        );

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            }],
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: sort_options_not,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options: sort_options,
                },
            ],
        ]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
            ]
        );

        let required_columns = [
            Arc::new(Column::new("b", 1)) as _,
            Arc::new(Column::new("a", 0)) as _,
        ];
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));

        // not satisfied orders
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (_, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0]);

        Ok(())
    }

    #[test]
    fn test_update_ordering() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]);

        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // b=a (e.g they are aliases)
        eq_properties.add_equal_conditions(col_b, col_a);
        // [b ASC], [d ASC]
        eq_properties.add_new_orderings(vec![
            vec![PhysicalSortExpr {
                expr: col_b.clone(),
                options: option_asc,
            }],
            vec![PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            }],
        ]);

        let test_cases = vec![
            // d + b
            (
                Arc::new(BinaryExpr::new(
                    col_d.clone(),
                    Operator::Plus,
                    col_b.clone(),
                )) as Arc<dyn PhysicalExpr>,
                SortProperties::Ordered(option_asc),
            ),
            // b
            (col_b.clone(), SortProperties::Ordered(option_asc)),
            // a
            (col_a.clone(), SortProperties::Ordered(option_asc)),
            // a + c
            (
                Arc::new(BinaryExpr::new(
                    col_a.clone(),
                    Operator::Plus,
                    col_c.clone(),
                )),
                SortProperties::Unordered,
            ),
        ];
        for (expr, expected) in test_cases {
            let leading_orderings = eq_properties
                .oeq_class()
                .iter()
                .flat_map(|ordering| ordering.first().cloned())
                .collect::<Vec<_>>();
            let expr_ordering = eq_properties.get_expr_ordering(expr.clone());
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}, leading_orderings: {leading_orderings:?}",
                expr, expected, expr_ordering.state
            );
            assert_eq!(expr_ordering.state, expected, "{}", err_msg);
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation_random() -> Result<()> {
        const N_RANDOM_SCHEMA: usize = 100;
        const N_ELEMENTS: usize = 125;
        const N_DISTINCT: usize = 5;

        for seed in 0..N_RANDOM_SCHEMA {
            // Create a random schema with random properties
            let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
            // Generate a data that satisfies properties given
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;

            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let exprs = vec![
                col("a", &test_schema)?,
                col("b", &test_schema)?,
                col("c", &test_schema)?,
                col("d", &test_schema)?,
                col("e", &test_schema)?,
                col("f", &test_schema)?,
                floor_a,
                a_plus_b,
            ];

            for n_req in 0..=exprs.len() {
                for exprs in exprs.iter().combinations(n_req) {
                    let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
                    let (ordering, indices) =
                        eq_properties.find_longest_permutation(&exprs);
                    // Make sure that find_longest_permutation return values are consistent
                    let ordering2 = indices
                        .iter()
                        .zip(ordering.iter())
                        .map(|(&idx, sort_expr)| PhysicalSortExpr {
                            expr: exprs[idx].clone(),
                            options: sort_expr.options,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        ordering, ordering2,
                        "indices and lexicographical ordering do not match"
                    );

                    let err_msg = format!(
                        "Error in test case ordering:{:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}",
                        ordering, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants
                    );
                    assert_eq!(ordering.len(), indices.len(), "{}", err_msg);
                    // Since ordered section satisfies schema, we expect
                    // that result will be same after sort (e.g sort was unnecessary).
                    assert!(
                        is_table_same_after_sort(
                            ordering.clone(),
                            table_data_with_properties.clone(),
                        )?,
                        "{}",
                        err_msg
                    );
                }
            }
        }

        Ok(())
    }
    #[test]
    fn test_find_longest_permutation() -> Result<()> {
        // Schema satisfies following orderings:
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        // and
        // Column [a=c] (e.g they are aliases).
        // At below we add [d ASC, h DESC] also, for test purposes
        let (test_schema, mut eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_h = &col("h", &test_schema)?;
        // a + d
        let a_plus_d = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // [d ASC, h ASC] also satisfies schema.
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            },
            PhysicalSortExpr {
                expr: col_h.clone(),
                options: option_desc,
            },
        ]]);
        let test_cases = vec![
            // TEST CASE 1
            (vec![col_a], vec![(col_a, option_asc)]),
            // TEST CASE 2
            (vec![col_c], vec![(col_c, option_asc)]),
            // TEST CASE 3
            (
                vec![col_d, col_e, col_b],
                vec![
                    (col_d, option_asc),
                    (col_e, option_desc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 4
            (vec![col_b], vec![]),
            // TEST CASE 5
            (vec![col_d], vec![(col_d, option_asc)]),
            // TEST CASE 5
            (vec![&a_plus_d], vec![(&a_plus_d, option_asc)]),
            // TEST CASE 6
            (
                vec![col_b, col_d],
                vec![(col_d, option_asc), (col_b, option_asc)],
            ),
            // TEST CASE 6
            (
                vec![col_c, col_e],
                vec![(col_c, option_asc), (col_e, option_desc)],
            ),
        ];
        for (exprs, expected) in test_cases {
            let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = convert_to_sort_exprs(&expected);
            let (actual, _) = eq_properties.find_longest_permutation(&exprs);
            assert_eq!(actual, expected);
        }

        Ok(())
    }
    #[test]
    fn test_get_meet_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let tests_cases = vec![
            // Get meet ordering between [a ASC] and [a ASC, b ASC]
            // result should be [a ASC]
            (
                vec![(col_a, option_asc)],
                vec![(col_a, option_asc), (col_b, option_asc)],
                Some(vec![(col_a, option_asc)]),
            ),
            // Get meet ordering between [a ASC] and [a DESC]
            // result should be None.
            (vec![(col_a, option_asc)], vec![(col_a, option_desc)], None),
            // Get meet ordering between [a ASC, b ASC] and [a ASC, b DESC]
            // result should be [a ASC].
            (
                vec![(col_a, option_asc), (col_b, option_asc)],
                vec![(col_a, option_asc), (col_b, option_desc)],
                Some(vec![(col_a, option_asc)]),
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_exprs(&lhs);
            let rhs = convert_to_sort_exprs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_exprs(&expected));
            let finer = eq_properties.get_meet_ordering(&lhs, &rhs);
            assert_eq!(finer, expected)
        }

        Ok(())
    }

    #[test]
    fn test_get_finer() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First entry, and second entry are the physical sort requirement that are argument for get_finer_requirement.
        // Third entry is the expected result.
        let tests_cases = vec![
            // Get finer requirement between [a Some(ASC)] and [a None, b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC)]
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, None), (col_b, Some(option_asc))],
                Some(vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC), c Some(ASC)] and [a Some(ASC), b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC), c Some(ASC)]
            (
                vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                Some(vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC)] and [a Some(ASC), b Some(DESC)]
            // result should be None
            (
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_desc))],
                None,
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_reqs(&lhs);
            let rhs = convert_to_sort_reqs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_reqs(&expected));
            let finer = eq_properties.get_finer_requirement(&lhs, &rhs);
            assert_eq!(finer, expected)
        }

        Ok(())
    }

    #[test]
    fn test_normalize_sort_reqs() -> Result<()> {
        // Schema satisfies following properties
        // a=c
        // and following orderings are valid
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First element in the tuple stores vector of requirement, second element is the expected return value for ordering_satisfy function
        let requirements = vec![
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (
                vec![(col_a, Some(option_desc))],
                vec![(col_a, Some(option_desc))],
            ),
            (vec![(col_a, None)], vec![(col_a, None)]),
            // Test whether equivalence works as expected
            (
                vec![(col_c, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (vec![(col_c, None)], vec![(col_a, None)]),
            // Test whether ordering equivalence works as expected
            (
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
            ),
            (
                vec![(col_d, None), (col_b, None)],
                vec![(col_d, None), (col_b, None)],
            ),
            (
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
            ),
            // We should be able to normalize in compatible requirements also (not exactly equal)
            (
                vec![(col_e, Some(option_desc)), (col_f, None)],
                vec![(col_e, Some(option_desc)), (col_f, None)],
            ),
            (
                vec![(col_e, None), (col_f, None)],
                vec![(col_e, None), (col_f, None)],
            ),
        ];

        for (reqs, expected_normalized) in requirements.into_iter() {
            let req = convert_to_sort_reqs(&reqs);
            let expected_normalized = convert_to_sort_reqs(&expected_normalized);

            assert_eq!(
                eq_properties.normalize_sort_requirements(&req),
                expected_normalized
            );
        }

        Ok(())
    }

    #[test]
    fn test_schema_normalize_sort_requirement_with_equivalence() -> Result<()> {
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Assume that column a and c are aliases.
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;

        // Test cases for equivalence normalization
        // First entry in the tuple is PhysicalSortRequirement, second entry in the tuple is
        // expected PhysicalSortRequirement after normalization.
        let test_cases = vec![
            (vec![(col_a, Some(option1))], vec![(col_a, Some(option1))]),
            // In the normalized version column c should be replace with column a
            (vec![(col_c, Some(option1))], vec![(col_a, Some(option1))]),
            (vec![(col_c, None)], vec![(col_a, None)]),
            (vec![(col_d, Some(option1))], vec![(col_d, Some(option1))]),
        ];
        for (reqs, expected) in test_cases.into_iter() {
            let reqs = convert_to_sort_reqs(&reqs);
            let expected = convert_to_sort_reqs(&expected);

            let normalized = eq_properties.normalize_sort_requirements(&reqs);
            assert!(
                expected.eq(&normalized),
                "error in test: reqs: {reqs:?}, expected: {expected:?}, normalized: {normalized:?}"
            );
        }

        Ok(())
    }
}
