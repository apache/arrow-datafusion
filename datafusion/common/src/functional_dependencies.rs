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

//! FunctionalDependencies keeps track of functional dependencies
//! inside DFSchema.

use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::vec::IntoIter;

use crate::error::_plan_err;
use crate::utils::{merge_and_order_indices, set_difference};
use crate::{DFSchema, DFSchemaRef, DataFusionError, JoinType, Result};

use sqlparser::ast::TableConstraint;

/// This object defines a constraint on a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Constraint {
    /// Columns with the given indices form a composite primary key (they are
    /// jointly unique and not nullable):
    PrimaryKey(Vec<usize>),
    /// Columns with the given indices form a composite unique key:
    Unique(Vec<usize>),
}

/// This object encapsulates a list of functional constraints:
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Constraints {
    inner: Vec<Constraint>,
}

impl Constraints {
    /// Create empty constraints
    pub fn empty() -> Self {
        Constraints::new_unverified(vec![])
    }

    /// Create a new `Constraints` object from the given `constraints`.
    /// Users should use the `empty` or `new_from_table_constraints` functions
    /// for constructing `Constraints`. This constructor is for internal
    /// purposes only and does not check whether the argument is valid. The user
    /// is responsible for supplying a valid vector of `Constraint` objects.
    pub fn new_unverified(constraints: Vec<Constraint>) -> Self {
        Self { inner: constraints }
    }

    /// Convert each `TableConstraint` to corresponding `Constraint`
    pub fn new_from_table_constraints(
        constraints: &[TableConstraint],
        df_schema: &DFSchemaRef,
    ) -> Result<Self> {
        let constraints = constraints
            .iter()
            .map(|c: &TableConstraint| match c {
                TableConstraint::Unique {
                    columns,
                    is_primary,
                    ..
                } => {
                    let field_names = df_schema.field_names();
                    // Get primary key and/or unique indices in the schema:
                    let indices = columns
                        .iter()
                        .map(|pk| {
                            let idx = field_names
                                .iter()
                                .position(|item| *item == pk.value)
                                .ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "Primary key doesn't exist".to_string(),
                                    )
                                })?;
                            Ok(idx)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(if *is_primary {
                        Constraint::PrimaryKey(indices)
                    } else {
                        Constraint::Unique(indices)
                    })
                }
                TableConstraint::ForeignKey { .. } => {
                    _plan_err!("Foreign key constraints are not currently supported")
                }
                TableConstraint::Check { .. } => {
                    _plan_err!("Check constraints are not currently supported")
                }
                TableConstraint::Index { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
                TableConstraint::FulltextOrSpatial { .. } => {
                    _plan_err!("Indexes are not currently supported")
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Constraints::new_unverified(constraints))
    }

    /// Check whether constraints is empty
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl IntoIterator for Constraints {
    type Item = Constraint;
    type IntoIter = IntoIter<Constraint>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl Display for Constraints {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let pk: Vec<String> = self.inner.iter().map(|c| format!("{:?}", c)).collect();
        let pk = pk.join(", ");
        if !pk.is_empty() {
            write!(f, " constraints=[{pk}]")
        } else {
            write!(f, "")
        }
    }
}

impl Deref for Constraints {
    type Target = [Constraint];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

/// This object defines a functional dependence in the schema. A functional
/// dependence defines a relationship between determinant keys and dependent
/// columns. A determinant key is a column, or a set of columns, whose value
/// uniquely determines values of some other (dependent) columns. If two rows
/// have the same determinant key, dependent columns in these rows are
/// necessarily the same. If the determinant key is unique, the set of
/// dependent columns is equal to the entire schema and the determinant key can
/// serve as a primary key. Note that a primary key may "downgrade" into a
/// determinant key due to an operation such as a join, and this object is
/// used to track dependence relationships in such cases. For more information
/// on functional dependencies, see:
/// <https://www.scaler.com/topics/dbms/functional-dependency-in-dbms/>
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionalDependence {
    // Column indices of the (possibly composite) determinant key:
    pub source_indices: Vec<usize>,
    // Column indices of dependent column(s):
    pub target_indices: Vec<usize>,
    /// Flag indicating whether one of the `source_indices` can receive NULL values.
    /// For a data source, if the constraint in question is `Constraint::Unique`,
    /// this flag is `true`. If the constraint in question is `Constraint::PrimaryKey`,
    /// this flag is `false`.
    /// Note that as the schema changes between different stages in a plan,
    /// such as after LEFT JOIN or RIGHT JOIN operations, this property may
    /// change.
    pub nullable: bool,
    // The functional dependency mode:
    pub mode: Dependency,
}

/// Describes functional dependency mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dependency {
    Single, // A determinant key may occur only once.
    Multi,  // A determinant key may occur multiple times (in multiple rows).
}

impl FunctionalDependence {
    // Creates a new functional dependence.
    pub fn new(
        source_indices: Vec<usize>,
        target_indices: Vec<usize>,
        nullable: bool,
    ) -> Self {
        Self {
            source_indices,
            target_indices,
            nullable,
            // Start with the least restrictive mode by default:
            mode: Dependency::Multi,
        }
    }

    pub fn with_mode(mut self, mode: Dependency) -> Self {
        self.mode = mode;
        self
    }
}

/// This object encapsulates all functional dependencies in a given relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionalDependencies {
    deps: Vec<FunctionalDependence>,
}

impl FunctionalDependencies {
    /// Creates an empty `FunctionalDependencies` object.
    pub fn empty() -> Self {
        Self { deps: vec![] }
    }

    /// Creates a new `FunctionalDependencies` object from a vector of
    /// `FunctionalDependence` objects.
    pub fn new(dependencies: Vec<FunctionalDependence>) -> Self {
        Self { deps: dependencies }
    }

    /// Creates a new `FunctionalDependencies` object from the given constraints.
    pub fn new_from_constraints(
        constraints: Option<&Constraints>,
        n_field: usize,
    ) -> Self {
        if let Some(Constraints { inner: constraints }) = constraints {
            // Construct dependency objects based on each individual constraint:
            let dependencies = constraints
                .iter()
                .map(|constraint| {
                    // All the field indices are associated with the whole table
                    // since we are dealing with table level constraints:
                    let dependency = match constraint {
                        Constraint::PrimaryKey(indices) => FunctionalDependence::new(
                            indices.to_vec(),
                            (0..n_field).collect::<Vec<_>>(),
                            false,
                        ),
                        Constraint::Unique(indices) => FunctionalDependence::new(
                            indices.to_vec(),
                            (0..n_field).collect::<Vec<_>>(),
                            true,
                        ),
                    };
                    // As primary keys are guaranteed to be unique, set the
                    // functional dependency mode to `Dependency::Single`:
                    dependency.with_mode(Dependency::Single)
                })
                .collect::<Vec<_>>();
            Self::new(dependencies)
        } else {
            // There is no constraint, return an empty object:
            Self::empty()
        }
    }

    pub fn with_dependency(mut self, mode: Dependency) -> Self {
        self.deps.iter_mut().for_each(|item| item.mode = mode);
        self
    }

    /// Merges the given functional dependencies with these.
    pub fn extend(&mut self, other: FunctionalDependencies) {
        self.deps.extend(other.deps);
    }

    /// Sanity checks if functional dependencies are valid. For example, if
    /// there are 10 fields, we cannot receive any index further than 9.
    pub fn is_valid(&self, n_field: usize) -> bool {
        self.deps.iter().all(
            |FunctionalDependence {
                 source_indices,
                 target_indices,
                 ..
             }| {
                source_indices
                    .iter()
                    .max()
                    .map(|&max_index| max_index < n_field)
                    .unwrap_or(true)
                    && target_indices
                        .iter()
                        .max()
                        .map(|&max_index| max_index < n_field)
                        .unwrap_or(true)
            },
        )
    }

    /// Adds the `offset` value to `source_indices` and `target_indices` for
    /// each functional dependency.
    pub fn add_offset(&mut self, offset: usize) {
        self.deps.iter_mut().for_each(
            |FunctionalDependence {
                 source_indices,
                 target_indices,
                 ..
             }| {
                *source_indices = add_offset_to_vec(source_indices, offset);
                *target_indices = add_offset_to_vec(target_indices, offset);
            },
        )
    }

    /// Updates `source_indices` and `target_indices` of each functional
    /// dependence using the index mapping given in `proj_indices`.
    ///
    /// Assume that `proj_indices` is \[2, 5, 8\] and we have a functional
    /// dependence \[5\] (`source_indices`) -> \[5, 8\] (`target_indices`).
    /// In the updated schema, fields at indices \[2, 5, 8\] will transform
    /// to \[0, 1, 2\]. Therefore, the resulting functional dependence will
    /// be \[1\] -> \[1, 2\].
    pub fn project_functional_dependencies(
        &self,
        proj_indices: &[usize],
        // The argument `n_out` denotes the schema field length, which is needed
        // to correctly associate a `Single`-mode dependence with the whole table.
        n_out: usize,
    ) -> FunctionalDependencies {
        let mut projected_func_dependencies = vec![];
        for FunctionalDependence {
            source_indices,
            target_indices,
            nullable,
            mode,
        } in &self.deps
        {
            let new_source_indices =
                update_elements_with_matching_indices(source_indices, proj_indices);
            let new_target_indices = if *mode == Dependency::Single {
                // Associate with all of the fields in the schema:
                (0..n_out).collect()
            } else {
                // Update associations according to projection:
                update_elements_with_matching_indices(target_indices, proj_indices)
            };
            // All of the composite indices should still be valid after projection;
            // otherwise, functional dependency cannot be propagated.
            if new_source_indices.len() == source_indices.len() {
                let new_func_dependence = FunctionalDependence::new(
                    new_source_indices,
                    new_target_indices,
                    *nullable,
                )
                .with_mode(*mode);
                projected_func_dependencies.push(new_func_dependence);
            }
        }
        FunctionalDependencies::new(projected_func_dependencies)
    }

    /// This function joins this set of functional dependencies with the `other`
    /// according to the given `join_type`.
    pub fn join(
        &self,
        other: &FunctionalDependencies,
        join_type: &JoinType,
        left_cols_len: usize,
    ) -> FunctionalDependencies {
        // Get mutable copies of left and right side dependencies:
        let mut right_func_dependencies = other.clone();
        let mut left_func_dependencies = self.clone();

        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right => {
                // Add offset to right schema:
                right_func_dependencies.add_offset(left_cols_len);

                // Result may have multiple values, update the dependency mode:
                left_func_dependencies =
                    left_func_dependencies.with_dependency(Dependency::Multi);
                right_func_dependencies =
                    right_func_dependencies.with_dependency(Dependency::Multi);

                if *join_type == JoinType::Left {
                    // Downgrade the right side, since it may have additional NULL values:
                    right_func_dependencies.downgrade_dependencies();
                } else if *join_type == JoinType::Right {
                    // Downgrade the left side, since it may have additional NULL values:
                    left_func_dependencies.downgrade_dependencies();
                }
                // Combine left and right functional dependencies:
                left_func_dependencies.extend(right_func_dependencies);
                left_func_dependencies
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // These joins preserve functional dependencies of the left side:
                left_func_dependencies
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                // These joins preserve functional dependencies of the right side:
                right_func_dependencies
            }
            JoinType::Full => {
                // All of the functional dependencies are lost in a FULL join:
                FunctionalDependencies::empty()
            }
        }
    }

    /// This function downgrades a functional dependency when nullability becomes
    /// a possibility:
    /// - If the dependency in question is UNIQUE (i.e. nullable), a new null value
    ///   invalidates the dependency.
    /// - If the dependency in question is PRIMARY KEY (i.e. not nullable), a new
    ///   null value turns it into UNIQUE mode.
    fn downgrade_dependencies(&mut self) {
        // Delete nullable dependencies, since they are no longer valid:
        self.deps.retain(|item| !item.nullable);
        self.deps.iter_mut().for_each(|item| item.nullable = true);
    }

    /// This function ensures that functional dependencies involving uniquely
    /// occuring determinant keys cover their entire table in terms of
    /// dependent columns.
    pub fn extend_target_indices(&mut self, n_out: usize) {
        self.deps.iter_mut().for_each(
            |FunctionalDependence {
                 mode,
                 target_indices,
                 ..
             }| {
                // If unique, cover the whole table:
                if *mode == Dependency::Single {
                    *target_indices = (0..n_out).collect::<Vec<_>>();
                }
            },
        )
    }
}

impl Deref for FunctionalDependencies {
    type Target = [FunctionalDependence];

    fn deref(&self) -> &Self::Target {
        self.deps.as_slice()
    }
}

/// Calculates functional dependencies for aggregate output, when there is a GROUP BY expression.
pub fn aggregate_functional_dependencies(
    aggr_input_schema: &DFSchema,
    group_by_expr_names: &[String],
    aggr_schema: &DFSchema,
) -> FunctionalDependencies {
    let mut aggregate_func_dependencies = vec![];
    let aggr_input_fields = aggr_input_schema.field_names();
    let aggr_fields = aggr_schema.fields();
    // Association covers the whole table:
    let target_indices = (0..aggr_schema.fields().len()).collect::<Vec<_>>();
    // Get functional dependencies of the schema:
    let func_dependencies = aggr_input_schema.functional_dependencies();
    for FunctionalDependence {
        source_indices,
        nullable,
        mode,
        ..
    } in &func_dependencies.deps
    {
        // Keep source indices in a `HashSet` to prevent duplicate entries:
        let mut new_source_indices = vec![];
        let mut new_source_field_names = vec![];
        let source_field_names = source_indices
            .iter()
            .map(|&idx| &aggr_input_fields[idx])
            .collect::<Vec<_>>();

        for (idx, group_by_expr_name) in group_by_expr_names.iter().enumerate() {
            // When one of the input determinant expressions matches with
            // the GROUP BY expression, add the index of the GROUP BY
            // expression as a new determinant key:
            if source_field_names.contains(&group_by_expr_name) {
                new_source_indices.push(idx);
                new_source_field_names.push(group_by_expr_name.clone());
            }
        }
        let existing_target_indices =
            get_target_functional_dependencies(aggr_input_schema, group_by_expr_names);
        let new_target_indices = get_target_functional_dependencies(
            aggr_input_schema,
            &new_source_field_names,
        );
        let mode = if existing_target_indices == new_target_indices
            && new_target_indices.is_some()
        {
            // If dependency covers all GROUP BY expressions, mode will be `Single`:
            Dependency::Single
        } else {
            // Otherwise, existing mode is preserved:
            *mode
        };
        // All of the composite indices occur in the GROUP BY expression:
        if new_source_indices.len() == source_indices.len() {
            aggregate_func_dependencies.push(
                FunctionalDependence::new(
                    new_source_indices,
                    target_indices.clone(),
                    *nullable,
                )
                .with_mode(mode),
            );
        }
    }

    // If we have a single GROUP BY key, we can guarantee uniqueness after
    // aggregation:
    if group_by_expr_names.len() == 1 {
        // If `source_indices` contain 0, delete this functional dependency
        // as it will be added anyway with mode `Dependency::Single`:
        aggregate_func_dependencies.retain(|item| !item.source_indices.contains(&0));
        // Add a new functional dependency associated with the whole table:
        aggregate_func_dependencies.push(
            // Use nullable property of the group by expression
            FunctionalDependence::new(
                vec![0],
                target_indices,
                aggr_fields[0].is_nullable(),
            )
            .with_mode(Dependency::Single),
        );
    }
    FunctionalDependencies::new(aggregate_func_dependencies)
}

/// Returns target indices, for the determinant keys that are inside
/// group by expressions.
pub fn get_target_functional_dependencies(
    schema: &DFSchema,
    group_by_expr_names: &[String],
) -> Option<Vec<usize>> {
    let mut combined_target_indices = HashSet::new();
    let dependencies = schema.functional_dependencies();
    let field_names = schema.field_names();
    for FunctionalDependence {
        source_indices,
        target_indices,
        ..
    } in &dependencies.deps
    {
        let source_key_names = source_indices
            .iter()
            .map(|id_key_idx| &field_names[*id_key_idx])
            .collect::<Vec<_>>();
        // If the GROUP BY expression contains a determinant key, we can use
        // the associated fields after aggregation even if they are not part
        // of the GROUP BY expression.
        if source_key_names
            .iter()
            .all(|source_key_name| group_by_expr_names.contains(source_key_name))
        {
            combined_target_indices.extend(target_indices.iter());
        }
    }
    (!combined_target_indices.is_empty()).then_some({
        let mut result = combined_target_indices.into_iter().collect::<Vec<_>>();
        result.sort();
        result
    })
}

/// Returns indices for the minimal subset of GROUP BY expressions that are
/// functionally equivalent to the original set of GROUP BY expressions.
pub fn get_required_group_by_exprs_indices(
    schema: &DFSchema,
    group_by_expr_names: &[String],
) -> Option<Vec<usize>> {
    let dependencies = schema.functional_dependencies();
    let field_names = schema.field_names();
    let mut groupby_expr_indices = group_by_expr_names
        .iter()
        .map(|group_by_expr_name| {
            field_names
                .iter()
                .position(|field_name| field_name == group_by_expr_name)
        })
        .collect::<Option<Vec<_>>>()?;

    groupby_expr_indices.sort();
    for FunctionalDependence {
        source_indices,
        target_indices,
        ..
    } in &dependencies.deps
    {
        if source_indices
            .iter()
            .all(|source_idx| groupby_expr_indices.contains(source_idx))
        {
            // If all source indices are among GROUP BY expression indices, we
            // can remove target indices from GROUP BY expression indices and
            // use source indices instead.
            groupby_expr_indices = set_difference(&groupby_expr_indices, target_indices);
            groupby_expr_indices =
                merge_and_order_indices(groupby_expr_indices, source_indices);
        }
    }
    groupby_expr_indices
        .iter()
        .map(|idx| {
            group_by_expr_names
                .iter()
                .position(|name| &field_names[*idx] == name)
        })
        .collect()
}

/// Updates entries inside the `entries` vector with their corresponding
/// indices inside the `proj_indices` vector.
fn update_elements_with_matching_indices(
    entries: &[usize],
    proj_indices: &[usize],
) -> Vec<usize> {
    entries
        .iter()
        .filter_map(|val| proj_indices.iter().position(|proj_idx| proj_idx == val))
        .collect()
}

/// Adds `offset` value to each entry inside `in_data`.
fn add_offset_to_vec<T: Copy + std::ops::Add<Output = T>>(
    in_data: &[T],
    offset: T,
) -> Vec<T> {
    in_data.iter().map(|&item| item + offset).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constraints_iter() {
        let constraints = Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![10]),
            Constraint::Unique(vec![20]),
        ]);
        let mut iter = constraints.iter();
        assert_eq!(iter.next(), Some(&Constraint::PrimaryKey(vec![10])));
        assert_eq!(iter.next(), Some(&Constraint::Unique(vec![20])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_get_updated_id_keys() {
        let fund_dependencies =
            FunctionalDependencies::new(vec![FunctionalDependence::new(
                vec![1],
                vec![0, 1, 2],
                true,
            )]);
        let res = fund_dependencies.project_functional_dependencies(&[1, 2], 2);
        let expected = FunctionalDependencies::new(vec![FunctionalDependence::new(
            vec![0],
            vec![0, 1],
            true,
        )]);
        assert_eq!(res, expected);
    }
}
