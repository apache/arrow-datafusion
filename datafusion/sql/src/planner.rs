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

//! [`SqlToRel`]: SQL Query Planner (produces [`LogicalPlan`] from SQL AST)
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use arrow_schema::*;
use datafusion_common::field_not_found;
use datafusion_common::internal_err;
use datafusion_common::logical_type::LogicalType;
use datafusion_common::logical_type::TypeSignature;
use datafusion_common::DFField;
use datafusion_expr::WindowUDF;
use sqlparser::ast::TimezoneInfo;
use sqlparser::ast::{ArrayElemTypeDef, ExactNumberInfo};
use sqlparser::ast::{ColumnDef as SQLColumnDef, ColumnOption};
use sqlparser::ast::{DataType as SQLDataType, Ident, ObjectName, TableAlias};

use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    not_impl_err, plan_err, unqualified_field_not_found, DFSchema, DataFusionError,
    Result,
};
use datafusion_common::{OwnedTableReference, TableReference};
use datafusion_expr::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion_expr::utils::find_column_exprs;
use datafusion_expr::TableSource;
use datafusion_expr::{col, AggregateUDF, Expr, ScalarUDF};

use crate::statement::object_name_to_string;
use crate::utils::make_decimal_type;

/// The ContextProvider trait allows the query planner to obtain meta-data about tables and
/// functions referenced in SQL statements
pub trait ContextProvider {
    #[deprecated(since = "32.0.0", note = "please use `get_table_source` instead")]
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        self.get_table_source(name)
    }
    /// Getter for a datasource
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>>;
    /// Getter for a UDF description
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>>;
    /// Getter for a UDAF description
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>>;
    /// Getter for a UDWF
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>>;
    /// Getter for system/user-defined variable type
    fn get_variable_type(&self, variable_names: &[String]) -> Option<LogicalType>;
    /// Getter for extension data type
    fn get_data_type(&self, _name: &TypeSignature) -> Option<LogicalType> {
        None
    }

    /// Get configuration options
    fn options(&self) -> &ConfigOptions;
}

/// SQL parser options
#[derive(Debug)]
pub struct ParserOptions {
    pub parse_float_as_decimal: bool,
    pub enable_ident_normalization: bool,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self {
            parse_float_as_decimal: false,
            enable_ident_normalization: true,
        }
    }
}

/// Ident Normalizer
#[derive(Debug)]
pub struct IdentNormalizer {
    normalize: bool,
}

impl Default for IdentNormalizer {
    fn default() -> Self {
        Self { normalize: true }
    }
}

impl IdentNormalizer {
    pub fn new(normalize: bool) -> Self {
        Self { normalize }
    }

    pub fn normalize(&self, ident: Ident) -> String {
        if self.normalize {
            crate::utils::normalize_ident(ident)
        } else {
            ident.value
        }
    }
}

/// Struct to store the states used by the Planner. The Planner will leverage the states to resolve
/// CTEs, Views, subqueries and PREPARE statements. The states include
/// Common Table Expression (CTE) provided with WITH clause and
/// Parameter Data Types provided with PREPARE statement and the query schema of the
/// outer query plan
#[derive(Debug, Clone)]
pub struct PlannerContext {
    /// Data types for numbered parameters ($1, $2, etc), if supplied
    /// in `PREPARE` statement
    prepare_param_data_types: Vec<LogicalType>,
    /// Map of CTE name to logical plan of the WITH clause.
    /// Use `Arc<LogicalPlan>` to allow cheap cloning
    ctes: HashMap<String, Arc<LogicalPlan>>,
    /// The query schema of the outer query plan, used to resolve the columns in subquery
    outer_query_schema: Option<DFSchema>,
}

impl Default for PlannerContext {
    fn default() -> Self {
        Self::new()
    }
}

impl PlannerContext {
    /// Create an empty PlannerContext
    pub fn new() -> Self {
        Self {
            prepare_param_data_types: vec![],
            ctes: HashMap::new(),
            outer_query_schema: None,
        }
    }

    /// Update the PlannerContext with provided prepare_param_data_types
    pub fn with_prepare_param_data_types(
        mut self,
        prepare_param_data_types: Vec<LogicalType>,
    ) -> Self {
        self.prepare_param_data_types = prepare_param_data_types;
        self
    }

    // return a reference to the outer queries schema
    pub fn outer_query_schema(&self) -> Option<&DFSchema> {
        self.outer_query_schema.as_ref()
    }

    /// sets the outer query schema, returning the existing one, if
    /// any
    pub fn set_outer_query_schema(
        &mut self,
        mut schema: Option<DFSchema>,
    ) -> Option<DFSchema> {
        std::mem::swap(&mut self.outer_query_schema, &mut schema);
        schema
    }

    /// Return the types of parameters (`$1`, `$2`, etc) if known
    pub fn prepare_param_data_types(&self) -> &[LogicalType] {
        &self.prepare_param_data_types
    }

    /// returns true if there is a Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn contains_cte(&self, cte_name: &str) -> bool {
        self.ctes.contains_key(cte_name)
    }

    /// Inserts a LogicalPlan for the Common Table Expression (CTE) /
    /// Subquery for the specified name
    pub fn insert_cte(&mut self, cte_name: impl Into<String>, plan: LogicalPlan) {
        let cte_name = cte_name.into();
        self.ctes.insert(cte_name, Arc::new(plan));
    }

    /// Return a plan for the Common Table Expression (CTE) / Subquery for the
    /// specified name
    pub fn get_cte(&self, cte_name: &str) -> Option<&LogicalPlan> {
        self.ctes.get(cte_name).map(|cte| cte.as_ref())
    }
}

/// SQL query planner
pub struct SqlToRel<'a, S: ContextProvider> {
    pub(crate) context_provider: &'a S,
    pub(crate) options: ParserOptions,
    pub(crate) normalizer: IdentNormalizer,
}

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    /// Create a new query planner
    pub fn new(context_provider: &'a S) -> Self {
        Self::new_with_options(context_provider, ParserOptions::default())
    }

    /// Create a new query planner
    pub fn new_with_options(context_provider: &'a S, options: ParserOptions) -> Self {
        let normalize = options.enable_ident_normalization;
        SqlToRel {
            context_provider,
            options,
            normalizer: IdentNormalizer::new(normalize),
        }
    }

    pub fn build_schema(&self, columns: Vec<SQLColumnDef>) -> Result<DFSchema> {
        let mut fields = Vec::with_capacity(columns.len());

        for column in columns {
            let data_type = self.convert_simple_data_type(&column.data_type)?;
            let not_nullable = column
                .options
                .iter()
                .any(|x| x.option == ColumnOption::NotNull);
            fields.push(DFField::new_unqualified(
                &self.normalizer.normalize(column.name),
                data_type,
                !not_nullable,
            ));
        }

        DFSchema::new_with_metadata(fields, Default::default())
    }

    /// Apply the given TableAlias to the input plan
    pub(crate) fn apply_table_alias(
        &self,
        plan: LogicalPlan,
        alias: TableAlias,
    ) -> Result<LogicalPlan> {
        let plan = self.apply_expr_alias(plan, alias.columns)?;

        LogicalPlanBuilder::from(plan)
            .alias(self.normalizer.normalize(alias.name))?
            .build()
    }

    pub(crate) fn apply_expr_alias(
        &self,
        plan: LogicalPlan,
        idents: Vec<Ident>,
    ) -> Result<LogicalPlan> {
        if idents.is_empty() {
            Ok(plan)
        } else if idents.len() != plan.schema().fields().len() {
            plan_err!(
                "Source table contains {} columns but only {} names given as column alias",
                plan.schema().fields().len(),
                idents.len()
            )
        } else {
            let fields = plan.schema().fields().clone();
            LogicalPlanBuilder::from(plan)
                .project(fields.iter().zip(idents.into_iter()).map(|(field, ident)| {
                    col(field.name()).alias(self.normalizer.normalize(ident))
                }))?
                .build()
        }
    }

    /// Validate the schema provides all of the columns referenced in the expressions.
    pub(crate) fn validate_schema_satisfies_exprs(
        &self,
        schema: &DFSchema,
        exprs: &[Expr],
    ) -> Result<()> {
        find_column_exprs(exprs)
            .iter()
            .try_for_each(|col| match col {
                Expr::Column(col) => match &col.relation {
                    Some(r) => {
                        schema.field_with_qualified_name(r, &col.name)?;
                        Ok(())
                    }
                    None => {
                        if !schema.fields_with_unqualified_name(&col.name).is_empty() {
                            Ok(())
                        } else {
                            Err(unqualified_field_not_found(col.name.as_str(), schema))
                        }
                    }
                }
                .map_err(|_: DataFusionError| {
                    field_not_found(col.relation.clone(), col.name.as_str(), schema)
                }),
                _ => internal_err!("Not a column"),
            })
    }

    pub(crate) fn convert_data_type(
        &self,
        sql_type: &SQLDataType,
    ) -> Result<LogicalType> {
        match sql_type {
            SQLDataType::Array(ArrayElemTypeDef::AngleBracket(inner_sql_type))
            | SQLDataType::Array(ArrayElemTypeDef::SquareBracket(inner_sql_type)) => {
                let data_type = self.convert_simple_data_type(inner_sql_type)?;

                Ok(LogicalType::List(Box::new(data_type)))
            }
            SQLDataType::Array(ArrayElemTypeDef::None) => {
                not_impl_err!("Arrays with unspecified type is not supported")
            }
            other => self.convert_simple_data_type(other),
        }
    }

    fn convert_simple_data_type(&self, sql_type: &SQLDataType) -> Result<LogicalType> {
        match sql_type {
            SQLDataType::Boolean | SQLDataType::Bool => Ok(LogicalType::Boolean),
            SQLDataType::TinyInt(_) => Ok(LogicalType::Int8),
            SQLDataType::SmallInt(_) | SQLDataType::Int2(_) => Ok(LogicalType::Int16),
            SQLDataType::Int(_) | SQLDataType::Integer(_) | SQLDataType::Int4(_) => Ok(LogicalType::Int32),
            SQLDataType::BigInt(_) | SQLDataType::Int8(_) => Ok(LogicalType::Int64),
            SQLDataType::UnsignedTinyInt(_) => Ok(LogicalType::UInt8),
            SQLDataType::UnsignedSmallInt(_) | SQLDataType::UnsignedInt2(_) => Ok(LogicalType::UInt16),
            SQLDataType::UnsignedInt(_) | SQLDataType::UnsignedInteger(_) | SQLDataType::UnsignedInt4(_) => {
                Ok(LogicalType::UInt32)
            }
            SQLDataType::UnsignedBigInt(_) | SQLDataType::UnsignedInt8(_) => Ok(LogicalType::UInt64),
            SQLDataType::Float(_) => Ok(LogicalType::Float32),
            SQLDataType::Real | SQLDataType::Float4 => Ok(LogicalType::Float32),
            SQLDataType::Double | SQLDataType::DoublePrecision | SQLDataType::Float8 => Ok(LogicalType::Float64),
            SQLDataType::Char(_)
            | SQLDataType::Varchar(_)
            | SQLDataType::Text
            | SQLDataType::String(_) => Ok(LogicalType::Utf8),
            SQLDataType::Timestamp(None, tz_info) => {
                let tz = if matches!(tz_info, TimezoneInfo::Tz)
                    || matches!(tz_info, TimezoneInfo::WithTimeZone)
                {
                    // Timestamp With Time Zone
                    // INPUT : [SQLDataType]   TimestampTz + [RuntimeConfig] Time Zone
                    // OUTPUT: [ArrowDataType] Timestamp<TimeUnit, Some(Time Zone)>
                    self.context_provider.options().execution.time_zone.clone()
                } else {
                    // Timestamp Without Time zone
                    None
                };
                Ok(LogicalType::Timestamp(TimeUnit::Nanosecond, tz.map(Into::into)))
            }
            SQLDataType::Date => Ok(LogicalType::Date32),
            SQLDataType::Time(None, tz_info) => {
                if matches!(tz_info, TimezoneInfo::None)
                    || matches!(tz_info, TimezoneInfo::WithoutTimeZone)
                {
                    Ok(LogicalType::Time64(TimeUnit::Nanosecond))
                } else {
                    // We dont support TIMETZ and TIME WITH TIME ZONE for now
                    not_impl_err!(
                        "Unsupported SQL type {sql_type:?}"
                    )
                }
            }
            SQLDataType::Numeric(exact_number_info)
            | SQLDataType::Decimal(exact_number_info) => {
                let (precision, scale) = match *exact_number_info {
                    ExactNumberInfo::None => (None, None),
                    ExactNumberInfo::Precision(precision) => (Some(precision), None),
                    ExactNumberInfo::PrecisionAndScale(precision, scale) => {
                        (Some(precision), Some(scale))
                    }
                };
                make_decimal_type(precision, scale)
            }
            SQLDataType::Bytea => Ok(LogicalType::Binary),
            SQLDataType::Interval => Ok(LogicalType::Interval(IntervalUnit::MonthDayNano)),
            // Explicitly list all other types so that if sqlparser
            // adds/changes the `SQLDataType` the compiler will tell us on upgrade
            // and avoid bugs like https://github.com/apache/arrow-datafusion/issues/3059
            SQLDataType::Custom(name, params) => {
                let name = object_name_to_string(name);
                let params = params.iter().map(Into::into).collect();
                let type_signature = TypeSignature::new_with_params(name, params);
                if let Some(data_type) = self.context_provider.get_data_type(&type_signature) {
                    return Ok(data_type);
                }

                plan_err!("User-Defined SQL type {sql_type:?} not found")
            }
            SQLDataType::Nvarchar(_)
            | SQLDataType::JSON
            | SQLDataType::Uuid
            | SQLDataType::Binary(_)
            | SQLDataType::Varbinary(_)
            | SQLDataType::Blob(_)
            | SQLDataType::Datetime(_)
            | SQLDataType::Regclass
            | SQLDataType::Array(_)
            | SQLDataType::Enum(_)
            | SQLDataType::Set(_)
            | SQLDataType::MediumInt(_)
            | SQLDataType::UnsignedMediumInt(_)
            | SQLDataType::Character(_)
            | SQLDataType::CharacterVarying(_)
            | SQLDataType::CharVarying(_)
            | SQLDataType::CharacterLargeObject(_)
            | SQLDataType::CharLargeObject(_)
            // precision is not supported
            | SQLDataType::Timestamp(Some(_), _)
            // precision is not supported
            | SQLDataType::Time(Some(_), _)
            | SQLDataType::Dec(_)
            | SQLDataType::BigNumeric(_)
            | SQLDataType::BigDecimal(_)
            | SQLDataType::Clob(_)
            | SQLDataType::Bytes(_)
            | SQLDataType::Int64
            | SQLDataType::Float64
            | SQLDataType::Struct(_)
            => not_impl_err!(
                "Unsupported SQL type {sql_type:?}"
            ),
        }
    }

    pub(crate) fn object_name_to_table_reference(
        &self,
        object_name: ObjectName,
    ) -> Result<OwnedTableReference> {
        object_name_to_table_reference(
            object_name,
            self.options.enable_ident_normalization,
        )
    }
}

/// Create a [`OwnedTableReference`] after normalizing the specified ObjectName
///
/// Examples
/// ```text
/// ['foo']          -> Bare { table: "foo" }
/// ['"foo.bar"]]    -> Bare { table: "foo.bar" }
/// ['foo', 'Bar']   -> Partial { schema: "foo", table: "bar" } <-- note lower case "bar"
/// ['foo', 'bar']   -> Partial { schema: "foo", table: "bar" }
/// ['foo', '"Bar"'] -> Partial { schema: "foo", table: "Bar" }
/// ```
pub fn object_name_to_table_reference(
    object_name: ObjectName,
    enable_normalization: bool,
) -> Result<OwnedTableReference> {
    // use destructure to make it clear no fields on ObjectName are ignored
    let ObjectName(idents) = object_name;
    idents_to_table_reference(idents, enable_normalization)
}

/// Create a [`OwnedTableReference`] after normalizing the specified identifier
pub(crate) fn idents_to_table_reference(
    idents: Vec<Ident>,
    enable_normalization: bool,
) -> Result<OwnedTableReference> {
    struct IdentTaker(Vec<Ident>);
    /// take the next identifier from the back of idents, panic'ing if
    /// there are none left
    impl IdentTaker {
        fn take(&mut self, enable_normalization: bool) -> String {
            let ident = self.0.pop().expect("no more identifiers");
            IdentNormalizer::new(enable_normalization).normalize(ident)
        }
    }

    let mut taker = IdentTaker(idents);

    match taker.0.len() {
        1 => {
            let table = taker.take(enable_normalization);
            Ok(OwnedTableReference::bare(table))
        }
        2 => {
            let table = taker.take(enable_normalization);
            let schema = taker.take(enable_normalization);
            Ok(OwnedTableReference::partial(schema, table))
        }
        3 => {
            let table = taker.take(enable_normalization);
            let schema = taker.take(enable_normalization);
            let catalog = taker.take(enable_normalization);
            Ok(OwnedTableReference::full(catalog, schema, table))
        }
        _ => plan_err!("Unsupported compound identifier '{:?}'", taker.0),
    }
}

/// Construct a WHERE qualifier suitable for e.g. information_schema filtering
/// from the provided object identifiers (catalog, schema and table names).
pub fn object_name_to_qualifier(
    sql_table_name: &ObjectName,
    enable_normalization: bool,
) -> String {
    let columns = vec!["table_name", "table_schema", "table_catalog"].into_iter();
    let normalizer = IdentNormalizer::new(enable_normalization);
    sql_table_name
        .0
        .iter()
        .rev()
        .zip(columns)
        .map(|(ident, column_name)| {
            format!(
                r#"{} = '{}'"#,
                column_name,
                normalizer.normalize(ident.clone())
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}
