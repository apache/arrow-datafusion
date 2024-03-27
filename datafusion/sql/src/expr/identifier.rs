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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow_schema::Field;
use datafusion_common::{
    internal_err, plan_datafusion_err, Column, DFSchema, DataFusionError,
    OwnedTableReference, Result, TableReference,
};
use datafusion_expr::{Case, Expr};
use sqlparser::ast::{Expr as SQLExpr, Ident};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_identifier_to_expr(
        &self,
        id: Ident,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if id.value.starts_with('@') {
            // TODO: figure out if ScalarVariables should be insensitive.
            let var_names = vec![id.value];
            let ty = self
                .context_provider
                .get_variable_type(&var_names)
                .ok_or_else(|| {
                    plan_datafusion_err!("variable {var_names:?} has no type information")
                })?;
            Ok(Expr::ScalarVariable(ty, var_names))
        } else {
            // Don't use `col()` here because it will try to
            // interpret names with '.' as if they were
            // compound identifiers, but this is not a compound
            // identifier. (e.g. it is "foo.bar" not foo.bar)
            let normalize_ident = self.normalizer.normalize(id);
            match schema.field_with_unqualified_name(normalize_ident.as_str()) {
                Ok(_) => {
                    // found a match without a qualified name, this is a inner table column
                    Ok(Expr::Column(Column {
                        relation: None,
                        name: normalize_ident,
                    }))
                }
                Err(_) => {
                    // check the outer_query_schema and try to find a match
                    if let Some(outer) = planner_context.outer_query_schema() {
                        match outer.qualified_field_with_unqualified_name(
                            normalize_ident.as_str(),
                        ) {
                            Ok((qualifier, field)) => {
                                // found an exact match on a qualified name in the outer plan schema, so this is an outer reference column
                                Ok(Expr::OuterReferenceColumn(
                                    field.data_type().clone(),
                                    Column::from((qualifier, field)),
                                ))
                            }
                            Err(_) => Ok(Expr::Column(Column {
                                relation: None,
                                name: normalize_ident,
                            })),
                        }
                    } else {
                        Ok(Expr::Column(Column {
                            relation: None,
                            name: normalize_ident,
                        }))
                    }
                }
            }
        }
    }

    pub(super) fn sql_compound_identifier_to_expr(
        &self,
        ids: Vec<Ident>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        if ids.len() < 2 {
            return internal_err!("Not a compound identifier: {ids:?}");
        }

        if ids[0].value.starts_with('@') {
            let var_names: Vec<_> = ids
                .into_iter()
                .map(|id| self.normalizer.normalize(id))
                .collect();
            let ty = self
                .context_provider
                .get_variable_type(&var_names)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "variable {var_names:?} has no type information"
                    ))
                })?;
            Ok(Expr::ScalarVariable(ty, var_names))
        } else {
            let ids = ids
                .into_iter()
                .map(|id| self.normalizer.normalize(id))
                .collect::<Vec<_>>();

            // Currently not supporting more than one nested level
            // Though ideally once that support is in place, this code should work with it
            // TODO: remove when can support multiple nested identifiers
            if ids.len() > 5 {
                return internal_err!("Unsupported compound identifier: {ids:?}");
            }

            let search_result = search_dfschema(&ids, schema);
            match search_result {
                // found matching field with spare identifier(s) for nested field(s) in structure
                Some((field, qualifier, nested_names)) if !nested_names.is_empty() => {
                    // TODO: remove when can support multiple nested identifiers
                    if nested_names.len() > 1 {
                        return internal_err!(
                            "Nested identifiers not yet supported for column {}",
                            Column::from((qualifier, field)).quoted_flat_name()
                        );
                    }
                    let nested_name = nested_names[0].to_string();
                    Ok(Expr::Column(Column::from((qualifier, field))).field(nested_name))
                }
                // found matching field with no spare identifier(s)
                Some((field, qualifier, _nested_names)) => {
                    Ok(Expr::Column(Column::from((qualifier, field))))
                }
                None => {
                    // return default where use all identifiers to not have a nested field
                    // this len check is because at 5 identifiers will have to have a nested field
                    if ids.len() == 5 {
                        internal_err!("Unsupported compound identifier: {ids:?}")
                    } else {
                        // check the outer_query_schema and try to find a match
                        if let Some(outer) = planner_context.outer_query_schema() {
                            let search_result = search_dfschema(&ids, outer);
                            match search_result {
                                // found matching field with spare identifier(s) for nested field(s) in structure
                                Some((field, qualifier, nested_names))
                                    if !nested_names.is_empty() =>
                                {
                                    // TODO: remove when can support nested identifiers for OuterReferenceColumn
                                    internal_err!(
                                        "Nested identifiers are not yet supported for OuterReferenceColumn {}",
                                        Column::from((qualifier, field)).quoted_flat_name()
                                    )
                                }
                                // found matching field with no spare identifier(s)
                                Some((field, qualifier, _nested_names)) => {
                                    // found an exact match on a qualified name in the outer plan schema, so this is an outer reference column
                                    Ok(Expr::OuterReferenceColumn(
                                        field.data_type().clone(),
                                        Column::from((qualifier, field)),
                                    ))
                                }
                                // found no matching field, will return a default
                                None => {
                                    let s = &ids[0..ids.len()];
                                    // safe unwrap as s can never be empty or exceed the bounds
                                    let (relation, column_name) =
                                        form_identifier(s).unwrap();
                                    let relation =
                                        relation.map(|r| r.to_owned_reference());
                                    Ok(Expr::Column(Column::new(relation, column_name)))
                                }
                            }
                        } else {
                            let s = &ids[0..ids.len()];
                            // safe unwrap as s can never be empty or exceed the bounds
                            let (relation, column_name) = form_identifier(s).unwrap();
                            let relation = relation.map(|r| r.to_owned_reference());
                            Ok(Expr::Column(Column::new(relation, column_name)))
                        }
                    }
                }
            }
        }
    }

    pub(super) fn sql_case_identifier_to_expr(
        &self,
        operand: Option<Box<SQLExpr>>,
        conditions: Vec<SQLExpr>,
        results: Vec<SQLExpr>,
        else_result: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let expr = if let Some(e) = operand {
            Some(Box::new(self.sql_expr_to_logical_expr(
                *e,
                schema,
                planner_context,
            )?))
        } else {
            None
        };
        let when_expr = conditions
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        let then_expr = results
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        let else_expr = if let Some(e) = else_result {
            Some(Box::new(self.sql_expr_to_logical_expr(
                *e,
                schema,
                planner_context,
            )?))
        } else {
            None
        };

        Ok(Expr::Case(Case::new(
            expr,
            when_expr
                .iter()
                .zip(then_expr.iter())
                .map(|(w, t)| (Box::new(w.to_owned()), Box::new(t.to_owned())))
                .collect(),
            else_expr,
        )))
    }
}

// (relation, column name)
fn form_identifier(idents: &[String]) -> Result<(Option<TableReference>, &String)> {
    match idents.len() {
        1 => Ok((None, &idents[0])),
        2 => Ok((
            Some(TableReference::Bare {
                table: (&idents[0]).into(),
            }),
            &idents[1],
        )),
        3 => Ok((
            Some(TableReference::Partial {
                schema: (&idents[0]).into(),
                table: (&idents[1]).into(),
            }),
            &idents[2],
        )),
        4 => Ok((
            Some(TableReference::Full {
                catalog: (&idents[0]).into(),
                schema: (&idents[1]).into(),
                table: (&idents[2]).into(),
            }),
            &idents[3],
        )),
        _ => internal_err!("Incorrect number of identifiers: {}", idents.len()),
    }
}

fn search_dfschema<'ids, 'schema>(
    ids: &'ids [String],
    schema: &'schema DFSchema,
) -> Option<(
    &'schema Field,
    Option<&'schema OwnedTableReference>,
    &'ids [String],
)> {
    generate_schema_search_terms(ids).find_map(|(qualifier, column, nested_names)| {
        let qualifier_and_field = schema
            .qualified_field_with_name(qualifier.as_ref(), column)
            .ok();
        qualifier_and_field.map(|(qualifier, field)| (field, qualifier, nested_names))
    })
}

// Possibilities we search with, in order from top to bottom for each len:
//
// len = 2:
// 1. (table.column)
// 2. (column).nested
//
// len = 3:
// 1. (schema.table.column)
// 2. (table.column).nested
// 3. (column).nested1.nested2
//
// len = 4:
// 1. (catalog.schema.table.column)
// 2. (schema.table.column).nested1
// 3. (table.column).nested1.nested2
// 4. (column).nested1.nested2.nested3
//
// len = 5:
// 1. (catalog.schema.table.column).nested
// 2. (schema.table.column).nested1.nested2
// 3. (table.column).nested1.nested2.nested3
// 4. (column).nested1.nested2.nested3.nested4
//
// len > 5:
// 1. (catalog.schema.table.column).nested[.nestedN]+
// 2. (schema.table.column).nested1.nested2[.nestedN]+
// 3. (table.column).nested1.nested2.nested3[.nestedN]+
// 4. (column).nested1.nested2.nested3.nested4[.nestedN]+
fn generate_schema_search_terms(
    ids: &[String],
) -> impl Iterator<Item = (Option<TableReference>, &String, &[String])> {
    // take at most 4 identifiers to form a Column to search with
    // - 1 for the column name
    // - 0 to 3 for the TableReference
    let bound = ids.len().min(4);
    // search terms from most specific to least specific
    (0..bound).rev().map(|i| {
        let nested_names_index = i + 1;
        let qualifier_and_column = &ids[0..nested_names_index];
        // safe unwrap as qualifier_and_column can never be empty or exceed the bounds
        let (relation, column_name) = form_identifier(qualifier_and_column).unwrap();
        (relation, column_name, &ids[nested_names_index..])
    })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    // testing according to documentation of generate_schema_search_terms function
    // where ensure generated search terms are in correct order with correct values
    fn test_generate_schema_search_terms() -> Result<()> {
        type ExpectedItem = (
            Option<TableReference<'static>>,
            &'static str,
            &'static [&'static str],
        );
        fn assert_vec_eq(
            expected: Vec<ExpectedItem>,
            actual: Vec<(Option<TableReference>, &String, &[String])>,
        ) {
            for (expected, actual) in expected.into_iter().zip(actual) {
                assert_eq!(expected.0, actual.0, "qualifier");
                assert_eq!(expected.1, actual.1, "column name");
                assert_eq!(expected.2, actual.2, "nested names");
            }
        }

        let actual = generate_schema_search_terms(&[]).collect::<Vec<_>>();
        assert!(actual.is_empty());

        let ids = vec!["a".to_string()];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![(None, "a", &[])];
        assert_vec_eq(expected, actual);

        let ids = vec!["a".to_string(), "b".to_string()];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![
            (Some(TableReference::bare("a")), "b", &[]),
            (None, "a", &["b"]),
        ];
        assert_vec_eq(expected, actual);

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![
            (Some(TableReference::partial("a", "b")), "c", &[]),
            (Some(TableReference::bare("a")), "b", &["c"]),
            (None, "a", &["b", "c"]),
        ];
        assert_vec_eq(expected, actual);

        let ids = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![
            (Some(TableReference::full("a", "b", "c")), "d", &[]),
            (Some(TableReference::partial("a", "b")), "c", &["d"]),
            (Some(TableReference::bare("a")), "b", &["c", "d"]),
            (None, "a", &["b", "c", "d"]),
        ];
        assert_vec_eq(expected, actual);

        let ids = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![
            (Some(TableReference::full("a", "b", "c")), "d", &["e"]),
            (Some(TableReference::partial("a", "b")), "c", &["d", "e"]),
            (Some(TableReference::bare("a")), "b", &["c", "d", "e"]),
            (None, "a", &["b", "c", "d", "e"]),
        ];
        assert_vec_eq(expected, actual);

        let ids = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
            "f".to_string(),
        ];
        let actual = generate_schema_search_terms(&ids).collect::<Vec<_>>();
        let expected: Vec<ExpectedItem> = vec![
            (Some(TableReference::full("a", "b", "c")), "d", &["e", "f"]),
            (
                Some(TableReference::partial("a", "b")),
                "c",
                &["d", "e", "f"],
            ),
            (Some(TableReference::bare("a")), "b", &["c", "d", "e", "f"]),
            (None, "a", &["b", "c", "d", "e", "f"]),
        ];
        assert_vec_eq(expected, actual);

        Ok(())
    }

    #[test]
    fn test_form_identifier() -> Result<()> {
        let err = form_identifier(&[]).expect_err("empty identifiers didn't fail");
        let expected = "Internal error: Incorrect number of identifiers: 0.\n\
        This was likely caused by a bug in DataFusion's code and we would \
        welcome that you file an bug report in our issue tracker";
        assert!(expected.starts_with(&err.strip_backtrace()));

        let ids = vec!["a".to_string()];
        let (qualifier, column) = form_identifier(&ids)?;
        assert_eq!(qualifier, None);
        assert_eq!(column, "a");

        let ids = vec!["a".to_string(), "b".to_string()];
        let (qualifier, column) = form_identifier(&ids)?;
        assert_eq!(qualifier, Some(TableReference::bare("a")));
        assert_eq!(column, "b");

        let ids = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let (qualifier, column) = form_identifier(&ids)?;
        assert_eq!(qualifier, Some(TableReference::partial("a", "b")));
        assert_eq!(column, "c");

        let ids = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let (qualifier, column) = form_identifier(&ids)?;
        assert_eq!(qualifier, Some(TableReference::full("a", "b", "c")));
        assert_eq!(column, "d");

        let err = form_identifier(&[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ])
        .expect_err("too many identifiers didn't fail");
        let expected = "Internal error: Incorrect number of identifiers: 5.\n\
        This was likely caused by a bug in DataFusion's code and we would \
        welcome that you file an bug report in our issue tracker";
        assert!(expected.starts_with(&err.strip_backtrace()));

        Ok(())
    }
}
