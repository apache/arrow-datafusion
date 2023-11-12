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

//! Conditional expressions
use crate::expr::Case;
use crate::{expr_schema::ExprSchemable, Expr};
use arrow::datatypes::DataType;
use datafusion_common::logical_type::LogicalType;
use datafusion_common::{plan_err, DFSchema, DataFusionError, Result};
use std::collections::HashSet;

/// Currently supported types by the coalesce function.
/// The order of these types correspond to the order on which coercion applies
/// This should thus be from least informative to most informative
pub static SUPPORTED_COALESCE_TYPES: &[DataType] = &[
    DataType::Boolean,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::Float32,
    DataType::Float64,
    DataType::Utf8,
    DataType::LargeUtf8,
];

/// Helper struct for building [Expr::Case]
pub struct CaseBuilder {
    expr: Option<Box<Expr>>,
    when_expr: Vec<Expr>,
    then_expr: Vec<Expr>,
    else_expr: Option<Box<Expr>>,
}

impl CaseBuilder {
    pub fn new(
        expr: Option<Box<Expr>>,
        when_expr: Vec<Expr>,
        then_expr: Vec<Expr>,
        else_expr: Option<Box<Expr>>,
    ) -> Self {
        Self {
            expr,
            when_expr,
            then_expr,
            else_expr,
        }
    }
    pub fn when(&mut self, when: Expr, then: Expr) -> CaseBuilder {
        self.when_expr.push(when);
        self.then_expr.push(then);
        CaseBuilder {
            expr: self.expr.clone(),
            when_expr: self.when_expr.clone(),
            then_expr: self.then_expr.clone(),
            else_expr: self.else_expr.clone(),
        }
    }
    pub fn otherwise(&mut self, else_expr: Expr) -> Result<Expr> {
        self.else_expr = Some(Box::new(else_expr));
        self.build()
    }

    pub fn end(&self) -> Result<Expr> {
        self.build()
    }

    fn build(&self) -> Result<Expr> {
        // collect all "then" expressions
        let mut then_expr = self.then_expr.clone();
        if let Some(e) = &self.else_expr {
            then_expr.push(e.as_ref().to_owned());
        }

        let then_types: Vec<LogicalType> = then_expr
            .iter()
            .map(|e| match e {
                Expr::Literal(_) => e.get_type(&DFSchema::empty()),
                _ => Ok(LogicalType::Null),
            })
            .collect::<Result<Vec<_>>>()?;

        if then_types.contains(&LogicalType::Null) {
            // cannot verify types until execution type
        } else {
            let unique_types: HashSet<&LogicalType> = then_types.iter().collect();
            if unique_types.len() != 1 {
                return plan_err!(
                    "CASE expression 'then' values had multiple data types: {unique_types:?}"
                );
            }
        }

        Ok(Expr::Case(Case::new(
            self.expr.clone(),
            self.when_expr
                .iter()
                .zip(self.then_expr.iter())
                .map(|(w, t)| (Box::new(w.clone()), Box::new(t.clone())))
                .collect(),
            self.else_expr.clone(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, lit, when};

    #[test]
    fn case_when_same_literal_then_types() -> Result<()> {
        let _ = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit(212))
            .end()?;
        Ok(())
    }

    #[test]
    fn case_when_different_literal_then_types() {
        let maybe_expr = when(col("state").eq(lit("CO")), lit(303))
            .when(col("state").eq(lit("NY")), lit("212"))
            .end();
        assert!(maybe_expr.is_err());
    }
}
