use std::sync::Arc;

use datafusion_expr::{expr::AggregateFunction, Expr};
use sqlparser::ast::NullTreatment;

use crate::first_last::first_value_udaf;

pub struct ExprBuilder {
    udf: Arc<crate::AggregateUDF>,
    /// List of expressions to feed to the functions as arguments
    args: Vec<Expr>,
    /// Whether this is a DISTINCT aggregation or not
    distinct: bool,
    /// Optional filter
    filter: Option<Box<Expr>>,
    /// Optional ordering
    order_by: Option<Vec<Expr>>,
    null_treatment: Option<NullTreatment>,
}

impl ExprBuilder {
    pub fn build(self) -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            self.udf,
            self.args,
            self.distinct,
            self.filter,
            self.order_by,
            self.null_treatment,
        ))
    }
    pub fn distinct(mut self) -> Self {
        self.distinct = true;
        self
    }
}

pub fn new_first_value(args: Vec<Expr>) -> ExprBuilder {
    ExprBuilder {
        udf: first_value_udaf(),
        args,
        distinct: false,
        filter: None,
        order_by: None,
        null_treatment: None,
    }
}
