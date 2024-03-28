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

//! [`DFParser`]: DataFusion SQL Parser based on [`sqlparser`]

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;

use datafusion_common::parsers::CompressionTypeVariant;
use sqlparser::{
    ast::{
        ColumnDef, ColumnOptionDef, ObjectName, OrderByExpr, Query,
        Statement as SQLStatement, TableConstraint, Value,
    },
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithLocation, Tokenizer, Word},
};

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

fn parse_file_type(s: &str) -> Result<String, ParserError> {
    Ok(s.to_uppercase())
}

/// DataFusion specific EXPLAIN (needed so we can EXPLAIN datafusion
/// specific COPY and other statements)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub verbose: bool,
    pub statement: Box<Statement>,
}

impl fmt::Display for ExplainStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            analyze,
            verbose,
            statement,
        } = self;

        write!(f, "EXPLAIN ")?;
        if *analyze {
            write!(f, "ANALYZE ")?;
        }
        if *verbose {
            write!(f, "VERBOSE ")?;
        }

        write!(f, "{statement}")
    }
}

/// DataFusion extension DDL for `COPY`
///
/// # Syntax:
///
/// ```text
/// COPY <table_name | (<query>)>
/// TO
/// <destination_url>
/// (key_value_list)
/// ```
///
/// # Examples
///
/// ```sql
/// COPY lineitem  TO 'lineitem'
///  (format parquet,
///   partitions 16,
///   row_group_limit_rows 100000,
///   row_group_limit_bytes 200000
///  )
///
/// COPY (SELECT l_orderkey from lineitem) to 'lineitem.parquet';
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyToStatement {
    /// From where the data comes from
    pub source: CopyToSource,
    /// The URL to where the data is heading
    pub target: String,
    /// Partition keys
    pub partitioned_by: Vec<String>,
    /// Indicates whether there is a header row (e.g. CSV)
    pub has_header: bool,
    /// File type (Parquet, NDJSON, CSV etc.)
    pub stored_as: Option<String>,
    /// Target specific options
    pub options: Vec<(String, Value)>,
}

impl fmt::Display for CopyToStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            source,
            target,
            partitioned_by,
            stored_as,
            options,
            ..
        } = self;

        write!(f, "COPY {source} TO {target}")?;
        if let Some(file_type) = stored_as {
            write!(f, " STORED AS {}", file_type)?;
        }
        if !partitioned_by.is_empty() {
            write!(f, " PARTITIONED BY ({})", partitioned_by.join(", "))?;
        }

        if self.has_header {
            write!(f, " WITH HEADER ROW")?;
        }

        if !options.is_empty() {
            let opts: Vec<_> = options.iter().map(|(k, v)| format!("{k} {v}")).collect();
            write!(f, " OPTIONS ({})", opts.join(", "))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    /// `COPY <table> TO ...`
    Relation(ObjectName),
    /// COPY (...query...) TO ...
    Query(Query),
}

impl fmt::Display for CopyToSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyToSource::Relation(r) => write!(f, "{r}"),
            CopyToSource::Query(q) => write!(f, "({q})"),
        }
    }
}

/// This type defines a lexicographical ordering.
pub(crate) type LexOrdering = Vec<OrderByExpr>;

/// DataFusion extension DDL for `CREATE EXTERNAL TABLE`
///
/// Syntax:
///
/// ```text
/// CREATE EXTERNAL TABLE
/// [ IF NOT EXISTS ]
/// <TABLE_NAME>[ (<column_definition>) ]
/// STORED AS <file_type>
/// [ WITH HEADER ROW ]
/// [ DELIMITER <char> ]
/// [ COMPRESSION TYPE <GZIP | BZIP2 | XZ | ZSTD> ]
/// [ PARTITIONED BY (<column_definition list> | <column list>) ]
/// [ WITH ORDER (<ordered column list>)
/// [ OPTIONS (<key_value_list>) ]
/// LOCATION <literal>
///
/// <column_definition> := (<column_name> <data_type>, ...)
///
/// <column_list> := (<column_name>, ...)
///
/// <ordered_column_list> := (<column_name> <sort_clause>, ...)
///
/// <key_value_list> := (<literal> <literal, <literal> <literal>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// Table name
    pub name: String,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    /// File type (Parquet, NDJSON, CSV, etc)
    pub file_type: String,
    /// CSV Header row?
    pub has_header: bool,
    /// User defined delimiter for CSVs
    pub delimiter: char,
    /// Path to file
    pub location: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Ordered expressions
    pub order_exprs: Vec<LexOrdering>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// File compression type (GZIP, BZIP2, XZ)
    pub file_compression_type: CompressionTypeVariant,
    /// Infinite streams?
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: HashMap<String, String>,
    /// A table-level constraint
    pub constraints: Vec<TableConstraint>,
}

impl fmt::Display for CreateExternalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", self.name)?;
        write!(f, "STORED AS {} ", self.file_type)?;
        write!(f, "LOCATION {} ", self.location)
    }
}

/// DataFusion SQL Statement.
///
/// This can either be a [`Statement`] from [`sqlparser`] from a
/// standard SQL dialect, or a DataFusion extension such as `CREATE
/// EXTERAL TABLE`. See [`DFParser`] for more information.
///
/// [`Statement`]: sqlparser::ast::Statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// ANSI SQL AST node (from sqlparser-rs)
    Statement(Box<SQLStatement>),
    /// Extension: `CREATE EXTERNAL TABLE`
    CreateExternalTable(CreateExternalTable),
    /// Extension: `COPY TO`
    CopyTo(CopyToStatement),
    /// EXPLAIN for extensions
    Explain(ExplainStatement),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Statement(stmt) => write!(f, "{stmt}"),
            Statement::CreateExternalTable(stmt) => write!(f, "{stmt}"),
            Statement::CopyTo(stmt) => write!(f, "{stmt}"),
            Statement::Explain(stmt) => write!(f, "{stmt}"),
        }
    }
}

fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), ParserError> {
    if field.is_some() {
        return Err(ParserError::ParserError(format!(
            "{name} specified more than once",
        )));
    }
    Ok(())
}

/// Datafusion SQL Parser based on [`sqlparser`]
///
/// Parses DataFusion's SQL dialect, often delegating to [`sqlparser`]'s [`Parser`].
///
/// DataFusion mostly follows existing SQL dialects via
/// `sqlparser`. However, certain statements such as `COPY` and
/// `CREATE EXTERNAL TABLE` have special syntax in DataFusion. See
/// [`Statement`] for a list of this special syntax
pub struct DFParser<'a> {
    pub parser: Parser<'a>,
}

impl<'a> DFParser<'a> {
    /// Create a new parser for the specified tokens using the
    /// [`GenericDialect`].
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::new_with_dialect(sql, dialect)
    }

    /// Create a new parser for the specified tokens with the
    /// specified dialect.
    pub fn new_with_dialect(
        sql: &str,
        dialect: &'a dyn Dialect,
    ) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(DFParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse a sql string into one or [`Statement`]s using the
    /// [`GenericDialect`].
    pub fn parse_sql(sql: &str) -> Result<VecDeque<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL string and produce one or more [`Statement`]s with
    /// with the specified dialect.
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<Statement>, ParserError> {
        let mut parser = DFParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(
        &self,
        expected: &str,
        found: TokenWithLocation,
    ) -> Result<T, ParserError> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token(); // CREATE
                        self.parse_create()
                    }
                    Keyword::COPY => {
                        self.parser.next_token(); // COPY
                        self.parse_copy()
                    }
                    Keyword::EXPLAIN => {
                        // (TODO parse all supported statements)
                        self.parser.next_token(); // EXPLAIN
                        self.parse_explain()
                    }
                    _ => {
                        // use sqlparser-rs parser
                        Ok(Statement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Statement(Box::from(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    /// Parse a SQL `COPY TO` statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        // parse as a query
        let source = if self.parser.consume_token(&Token::LParen) {
            let query = self.parser.parse_query()?;
            self.parser.expect_token(&Token::RParen)?;
            CopyToSource::Query(query)
        } else {
            // parse as table reference
            let table_name = self.parser.parse_object_name(true)?;
            CopyToSource::Relation(table_name)
        };

        #[derive(Default)]
        struct Builder {
            stored_as: Option<String>,
            target: Option<String>,
            partitioned_by: Option<Vec<String>>,
            has_header: Option<bool>,
            options: Option<Vec<(String, Value)>>,
        }

        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::TO,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
                Keyword::WITH,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.stored_as, "STORED AS")?;
                        builder.stored_as = Some(self.parse_file_format()?);
                    }
                    Keyword::TO => {
                        ensure_not_set(&builder.target, "TO")?;
                        builder.target = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        self.parser.expect_keyword(Keyword::HEADER)?;
                        self.parser.expect_keyword(Keyword::ROW)?;
                        ensure_not_set(&builder.has_header, "WITH HEADER ROW")?;
                        builder.has_header = Some(true);
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.partitioned_by, "PARTITIONED BY")?;
                        builder.partitioned_by = Some(self.parse_partitions()?);
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_value_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.next_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token {token}"
                    )));
                }
            }
        }

        let Some(target) = builder.target else {
            return Err(ParserError::ParserError(
                "Missing TO clause in COPY statement".into(),
            ));
        };

        Ok(Statement::CopyTo(CopyToStatement {
            source,
            target,
            partitioned_by: builder.partitioned_by.unwrap_or(vec![]),
            has_header: builder.has_header.unwrap_or(false),
            stored_as: builder.stored_as,
            options: builder.options.unwrap_or(vec![]),
        }))
    }

    /// Parse the next token as a key name for an option list
    ///
    /// Note this is different than [`parse_literal_string`]
    /// because it allows keywords as well as other non words
    ///
    /// [`parse_literal_string`]: sqlparser::parser::Parser::parse_literal_string
    pub fn parse_option_key(&mut self) -> Result<String, ParserError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            Token::Word(Word { value, .. }) => Ok(value),
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) => Ok(s),
            _ => self.parser.expected("key name", next_token),
        }
    }

    /// Parse the next token as a value for an option list
    ///
    /// Note this is different than [`parse_value`] as it allows any
    /// word or keyword in this location.
    ///
    /// [`parse_value`]: sqlparser::parser::Parser::parse_value
    pub fn parse_option_value(&mut self) -> Result<Value, ParserError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            Token::Word(Word { value, .. }) => Ok(Value::UnQuotedString(value)),
            Token::SingleQuotedString(s) => Ok(Value::SingleQuotedString(s)),
            Token::DoubleQuotedString(s) => Ok(Value::DoubleQuotedString(s)),
            Token::EscapedStringLiteral(s) => Ok(Value::EscapedStringLiteral(s)),
            Token::Number(ref n, l) => match n.parse() {
                Ok(n) => Ok(Value::Number(n, l)),
                // The tokenizer should have ensured `n` is an integer
                // so this should not be possible
                Err(e) => parser_err!(format!(
                    "Unexpected error: could not parse '{n}' as number: {e}"
                )),
            },
            _ => self.parser.expected("string or numeric value", next_token),
        }
    }

    /// Parse a SQL `EXPLAIN`
    pub fn parse_explain(&mut self) -> Result<Statement, ParserError> {
        let analyze = self.parser.parse_keyword(Keyword::ANALYZE);
        let verbose = self.parser.parse_keyword(Keyword::VERBOSE);
        let statement = self.parse_statement()?;

        Ok(Statement::Explain(ExplainStatement {
            statement: Box::new(statement),
            analyze,
            verbose,
        }))
    }

    /// Parse a SQL `CREATE` statement handling `CREATE EXTERNAL TABLE`
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(false)
        } else if self.parser.parse_keyword(Keyword::UNBOUNDED) {
            self.parser.expect_keyword(Keyword::EXTERNAL)?;
            self.parse_create_external_table(true)
        } else {
            Ok(Statement::Statement(Box::from(self.parser.parse_create()?)))
        }
    }

    fn parse_partitions(&mut self) -> Result<Vec<String>, ParserError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token().token {
                let identifier = self.parser.parse_identifier(false)?;
                partitions.push(identifier.to_string());
            } else {
                return self.expected("partition name", self.parser.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after partition definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(partitions)
    }

    /// Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
    pub fn parse_order_by_exprs(&mut self) -> Result<Vec<OrderByExpr>, ParserError> {
        let mut values = vec![];
        self.parser.expect_token(&Token::LParen)?;
        loop {
            values.push(self.parse_order_by_expr()?);
            if !self.parser.consume_token(&Token::Comma) {
                self.parser.expect_token(&Token::RParen)?;
                return Ok(values);
            }
        }
    }

    /// Parse an ORDER BY sub-expression optionally followed by ASC or DESC.
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parser.parse_expr()?;

        let asc = if self.parser.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parser.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self
            .parser
            .parse_keywords(&[Keyword::NULLS, Keyword::FIRST])
        {
            Some(true)
        } else if self.parser.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
        })
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(
        &mut self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token().token {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier(false)?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name(false)?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier(false)?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create_external_table(
        &mut self,
        unbounded: bool,
    ) -> Result<Statement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name(true)?;
        let (mut columns, constraints) = self.parse_columns()?;

        #[derive(Default)]
        struct Builder {
            file_type: Option<String>,
            location: Option<String>,
            has_header: Option<bool>,
            delimiter: Option<char>,
            file_compression_type: Option<CompressionTypeVariant>,
            table_partition_cols: Option<Vec<String>>,
            order_exprs: Vec<LexOrdering>,
            options: Option<HashMap<String, String>>,
        }
        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
                Keyword::DELIMITER,
                Keyword::COMPRESSION,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.file_type, "STORED AS")?;
                        builder.file_type = Some(self.parse_file_format()?);
                    }
                    Keyword::LOCATION => {
                        ensure_not_set(&builder.location, "LOCATION")?;
                        builder.location = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        if self.parser.parse_keyword(Keyword::ORDER) {
                            builder.order_exprs.push(self.parse_order_by_exprs()?);
                        } else {
                            self.parser.expect_keyword(Keyword::HEADER)?;
                            self.parser.expect_keyword(Keyword::ROW)?;
                            ensure_not_set(&builder.has_header, "WITH HEADER ROW")?;
                            builder.has_header = Some(true);
                        }
                    }
                    Keyword::DELIMITER => {
                        ensure_not_set(&builder.delimiter, "DELIMITER")?;
                        builder.delimiter = Some(self.parse_delimiter()?);
                    }
                    Keyword::COMPRESSION => {
                        self.parser.expect_keyword(Keyword::TYPE)?;
                        ensure_not_set(
                            &builder.file_compression_type,
                            "COMPRESSION TYPE",
                        )?;
                        builder.file_compression_type =
                            Some(self.parse_file_compression_type()?);
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.table_partition_cols, "PARTITIONED BY")?;
                        // Expects either list of column names (col_name [, col_name]*)
                        // or list of column definitions (col_name datatype [, col_name datatype]* )
                        // use the token after the name to decide which parsing rule to use
                        // Note that mixing both names and definitions is not allowed
                        let peeked = self.parser.peek_nth_token(2);
                        if peeked == Token::Comma || peeked == Token::RParen {
                            // list of column names
                            builder.table_partition_cols = Some(self.parse_partitions()?)
                        } else {
                            // list of column defs
                            let (cols, cons) = self.parse_columns()?;
                            builder.table_partition_cols = Some(
                                cols.iter().map(|col| col.name.to_string()).collect(),
                            );

                            columns.extend(cols);

                            if !cons.is_empty() {
                                return Err(ParserError::ParserError(
                                    "Constraints on Partition Columns are not supported"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_string_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.next_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token {token}"
                    )));
                }
            }
        }

        // Validations: location and file_type are required
        if builder.file_type.is_none() {
            return Err(ParserError::ParserError(
                "Missing STORED AS clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }
        if builder.location.is_none() {
            return Err(ParserError::ParserError(
                "Missing LOCATION clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }

        let create = CreateExternalTable {
            name: table_name.to_string(),
            columns,
            file_type: builder.file_type.unwrap(),
            has_header: builder.has_header.unwrap_or(false),
            delimiter: builder.delimiter.unwrap_or(','),
            location: builder.location.unwrap(),
            table_partition_cols: builder.table_partition_cols.unwrap_or(vec![]),
            order_exprs: builder.order_exprs,
            if_not_exists,
            file_compression_type: builder
                .file_compression_type
                .unwrap_or(CompressionTypeVariant::UNCOMPRESSED),
            unbounded,
            options: builder.options.unwrap_or(HashMap::new()),
            constraints,
        };
        Ok(Statement::CreateExternalTable(create))
    }

    /// Parses the set of valid formats
    fn parse_file_format(&mut self) -> Result<String, ParserError> {
        let token = self.parser.next_token();
        match &token.token {
            Token::Word(w) => parse_file_type(&w.value),
            _ => self.expected("one of ARROW, PARQUET, NDJSON, or CSV", token),
        }
    }

    /// Parses the set of
    fn parse_file_compression_type(
        &mut self,
    ) -> Result<CompressionTypeVariant, ParserError> {
        let token = self.parser.next_token();
        match &token.token {
            Token::Word(w) => CompressionTypeVariant::from_str(&w.value),
            _ => self.expected("one of GZIP, BZIP2, XZ, ZSTD", token),
        }
    }

    /// Parses (key value) style options where the values are literal strings.
    fn parse_string_options(&mut self) -> Result<HashMap<String, String>, ParserError> {
        let mut options = HashMap::new();
        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parser.parse_literal_string()?;
            let value = self.parser.parse_literal_string()?;
            options.insert(key, value);
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(options)
    }

    /// Parses (key value) style options into a map of String --> [`Value`].
    ///
    /// Unlike [`Self::parse_string_options`], this method supports
    /// keywords as key names as well as multiple value types such as
    /// Numbers as well as Strings.
    fn parse_value_options(&mut self) -> Result<Vec<(String, Value)>, ParserError> {
        let mut options = vec![];
        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parse_option_key()?;
            let value = self.parse_option_value()?;
            options.push((key, value));
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(options)
    }

    fn parse_delimiter(&mut self) -> Result<char, ParserError> {
        let token = self.parser.parse_literal_string()?;
        match token.len() {
            1 => Ok(token.chars().next().unwrap()),
            _ => Err(ParserError::TokenizerError(
                "Delimiter must be a single char".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Expr::Identifier;
    use sqlparser::ast::{BinaryOperator, DataType, Expr, Ident};
    use CompressionTypeVariant::UNCOMPRESSED;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), ParserError> {
        let statements = DFParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected, "actual:\n{:#?}", statements[0]);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) {
        match DFParser::parse_sql(sql) {
            Ok(statements) => {
                panic!(
                    "Expected parse error for '{sql}', but was successful: {statements:?}"
                );
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{expected_error}' not found in actual error '{error_message}'"
                );
            }
        }
    }

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
            },
            data_type,
            collation: None,
            options: vec![],
        }
    }

    #[test]
    fn create_external_table() -> Result<(), ParserError> {
        // positive case
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: leading space
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'     ";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(None))],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: leading space + semicolon
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'      ;";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(None))],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case with delimiter
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV DELIMITER '|' LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: '|',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: partitioned by
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1, p2) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec!["p1".to_string(), "p2".to_string()],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for case insensitive sql stmt with `WITH HEADER ROW` tokens
        let sqls = vec![
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH HEADER ROW LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV with header row LOCATION 'foo.csv'"
        ];
        for sql in sqls {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                name: "t".into(),
                columns: vec![make_column_def("c1", DataType::Int(display))],
                file_type: "CSV".to_string(),
                has_header: true,
                delimiter: ',',
                location: "foo.csv".into(),
                table_partition_cols: vec![],
                order_exprs: vec![],
                if_not_exists: false,
                file_compression_type: UNCOMPRESSED,
                unbounded: false,
                options: HashMap::new(),
                constraints: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // positive case: it is ok for sql stmt with `COMPRESSION TYPE GZIP` tokens
        let sqls = vec![
            ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE GZIP LOCATION 'foo.csv'", "GZIP"),
            ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE BZIP2 LOCATION 'foo.csv'", "BZIP2"),
            ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE XZ LOCATION 'foo.csv'", "XZ"),
            ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV COMPRESSION TYPE ZSTD LOCATION 'foo.csv'", "ZSTD"),
        ];
        for (sql, file_compression_type) in sqls {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                name: "t".into(),
                columns: vec![make_column_def("c1", DataType::Int(display))],
                file_type: "CSV".to_string(),
                has_header: false,
                delimiter: ',',
                location: "foo.csv".into(),
                table_partition_cols: vec![],
                order_exprs: vec![],
                if_not_exists: false,
                file_compression_type: CompressionTypeVariant::from_str(
                    file_compression_type,
                )?,
                unbounded: false,
                options: HashMap::new(),
                constraints: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files to be other than upper case
        let sql = "CREATE EXTERNAL TABLE t STORED AS parqueT LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS AVRO LOCATION 'foo.avro'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "AVRO".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.avro".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql =
            "CREATE EXTERNAL TABLE IF NOT EXISTS t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: true,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: column definiton allowed in 'partition by' clause
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![
                make_column_def("c1", DataType::Int(None)),
                make_column_def("p1", DataType::Int(None)),
            ],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec!["p1".to_string()],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int, c1) LOCATION 'foo.csv'";
        expect_parse_error(sql, "sql parser error: Expected a data type name, found: )");

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (c1, p1 int) LOCATION 'foo.csv'";
        expect_parse_error(sql, "sql parser error: Expected ',' or ')' after partition definition, found: int");

        // positive case: additional options (one entry) can be specified
        let sql =
            "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1') LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "X".to_string(),
            has_header: false,
            delimiter: ',',
            location: "blahblah".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::from([("k1".into(), "v1".into())]),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: additional options (multiple entries) can be specified
        let sql =
            "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1', k2 v2) LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![],
            file_type: "X".to_string(),
            has_header: false,
            delimiter: ',',
            location: "blahblah".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::from([
                ("k1".into(), "v1".into()),
                ("k2".into(), "v2".into()),
            ]),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // Ordered Col
        let sqls = ["CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 NULLS FIRST) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 NULLS LAST) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC NULLS FIRST) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC NULLS LAST) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC NULLS FIRST) LOCATION 'foo.csv'",
                        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC NULLS LAST) LOCATION 'foo.csv'"];
        let expected = vec![
            (None, None),
            (None, Some(true)),
            (None, Some(false)),
            (Some(true), None),
            (Some(false), None),
            (Some(false), Some(true)),
            (Some(false), Some(false)),
            (Some(true), Some(true)),
            (Some(true), Some(false)),
        ];
        for (sql, (asc, nulls_first)) in sqls.iter().zip(expected.into_iter()) {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                name: "t".into(),
                columns: vec![make_column_def("c1", DataType::Int(None))],
                file_type: "CSV".to_string(),
                has_header: false,
                delimiter: ',',
                location: "foo.csv".into(),
                table_partition_cols: vec![],
                order_exprs: vec![vec![OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                    }),
                    asc,
                    nulls_first,
                }]],
                if_not_exists: false,
                file_compression_type: UNCOMPRESSED,
                unbounded: false,
                options: HashMap::new(),
                constraints: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // Ordered Col
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 ASC, c2 DESC NULLS FIRST) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![
                make_column_def("c1", DataType::Int(display)),
                make_column_def("c2", DataType::Int(display)),
            ],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![vec![
                OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                    }),
                    asc: Some(true),
                    nulls_first: None,
                },
                OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                    }),
                    asc: Some(false),
                    nulls_first: Some(true),
                },
            ]],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // Ordered Binary op
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 - c2 ASC) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![
                make_column_def("c1", DataType::Int(display)),
                make_column_def("c2", DataType::Int(display)),
            ],
            file_type: "CSV".to_string(),
            has_header: false,
            delimiter: ',',
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Minus,
                    right: Box::new(Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                    })),
                },
                asc: Some(true),
                nulls_first: None,
            }]],
            if_not_exists: false,
            file_compression_type: UNCOMPRESSED,
            unbounded: false,
            options: HashMap::new(),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // Most complete CREATE EXTERNAL TABLE statement possible
        let sql = "
            CREATE UNBOUNDED EXTERNAL TABLE IF NOT EXISTS t (c1 int, c2 float)
            STORED AS PARQUET
            DELIMITER '*'
            WITH HEADER ROW
            WITH ORDER (c1 - c2 ASC)
            COMPRESSION TYPE zstd
            PARTITIONED BY (c1)
            LOCATION 'foo.parquet'
            OPTIONS (ROW_GROUP_SIZE '1024', 'TRUNCATE' 'NO')
        ";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: "t".into(),
            columns: vec![
                make_column_def("c1", DataType::Int(None)),
                make_column_def("c2", DataType::Float(None)),
            ],
            file_type: "PARQUET".to_string(),
            has_header: true,
            delimiter: '*',
            location: "foo.parquet".into(),
            table_partition_cols: vec!["c1".into()],
            order_exprs: vec![vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                    })),
                    op: BinaryOperator::Minus,
                    right: Box::new(Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                    })),
                },
                asc: Some(true),
                nulls_first: None,
            }]],
            if_not_exists: true,
            file_compression_type: CompressionTypeVariant::ZSTD,
            unbounded: true,
            options: HashMap::from([
                ("ROW_GROUP_SIZE".into(), "1024".into()),
                ("TRUNCATE".into(), "NO".into()),
            ]),
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // For error cases, see: `create_external_table.slt`

        Ok(())
    }

    #[test]
    fn copy_to_table_to_table() -> Result<(), ParserError> {
        // positive case
        let sql = "COPY foo TO bar STORED AS CSV";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec![],
            has_header: false,
            stored_as: Some("CSV".to_owned()),
            options: vec![],
        });

        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn explain_copy_to_table_to_table() -> Result<(), ParserError> {
        let cases = vec![
            ("EXPLAIN COPY foo TO bar STORED AS PARQUET", false, false),
            (
                "EXPLAIN ANALYZE COPY foo TO bar STORED AS PARQUET",
                true,
                false,
            ),
            (
                "EXPLAIN VERBOSE COPY foo TO bar STORED AS PARQUET",
                false,
                true,
            ),
            (
                "EXPLAIN ANALYZE VERBOSE COPY foo TO bar STORED AS PARQUET",
                true,
                true,
            ),
        ];
        for (sql, analyze, verbose) in cases {
            println!("sql: {sql}, analyze: {analyze}, verbose: {verbose}");

            let expected_copy = Statement::CopyTo(CopyToStatement {
                source: object_name("foo"),
                target: "bar".to_string(),
                partitioned_by: vec![],
                has_header: false,
                stored_as: Some("PARQUET".to_owned()),
                options: vec![],
            });
            let expected = Statement::Explain(ExplainStatement {
                analyze,
                verbose,
                statement: Box::new(expected_copy),
            });
            assert_eq!(verified_stmt(sql), expected);
        }
        Ok(())
    }

    #[test]
    fn copy_to_query_to_table() -> Result<(), ParserError> {
        let statement = verified_stmt("SELECT 1");

        // unwrap the various layers
        let statement = if let Statement::Statement(statement) = statement {
            *statement
        } else {
            panic!("Expected statement, got {statement:?}");
        };

        let query = if let SQLStatement::Query(query) = statement {
            *query
        } else {
            panic!("Expected query, got {statement:?}");
        };

        let sql = "COPY (SELECT 1) TO bar STORED AS CSV WITH HEADER ROW";
        let expected = Statement::CopyTo(CopyToStatement {
            source: CopyToSource::Query(query),
            target: "bar".to_string(),
            partitioned_by: vec![],
            has_header: true,
            stored_as: Some("CSV".to_owned()),
            options: vec![],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_options() -> Result<(), ParserError> {
        let sql = "COPY foo TO bar STORED AS CSV OPTIONS (row_group_size 55)";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec![],
            has_header: false,
            stored_as: Some("CSV".to_owned()),
            options: vec![(
                "row_group_size".to_string(),
                Value::Number("55".to_string(), false),
            )],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_partitioned_by() -> Result<(), ParserError> {
        let sql = "COPY foo TO bar STORED AS CSV PARTITIONED BY (a) OPTIONS (row_group_size 55)";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec!["a".to_string()],
            has_header: false,
            stored_as: Some("CSV".to_owned()),
            options: vec![(
                "row_group_size".to_string(),
                Value::Number("55".to_string(), false),
            )],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_multi_options() -> Result<(), ParserError> {
        // order of options is preserved
        let sql =
            "COPY foo TO bar STORED AS parquet OPTIONS ('format.row_group_size' 55, 'format.compression' snappy)";

        let expected_options = vec![
            (
                "format.row_group_size".to_string(),
                Value::Number("55".to_string(), false),
            ),
            (
                "format.compression".to_string(),
                Value::UnQuotedString("snappy".to_string()),
            ),
        ];

        let mut statements = DFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let only_statement = statements.pop_front().unwrap();

        let options = if let Statement::CopyTo(copy_to) = only_statement {
            copy_to.options
        } else {
            panic!("Expected copy");
        };

        assert_eq!(options, expected_options);

        Ok(())
    }

    // For error cases, see: `copy.slt`

    fn object_name(name: &str) -> CopyToSource {
        CopyToSource::Relation(ObjectName(vec![Ident::new(name)]))
    }

    // Based on  sqlparser-rs
    // https://github.com/sqlparser-rs/sqlparser-rs/blob/ae3b5844c839072c235965fe0d1bddc473dced87/src/test_utils.rs#L104-L116

    /// Ensures that `sql` parses as a single [Statement]
    ///
    /// If `canonical` is non empty,this function additionally asserts
    /// that:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    /// `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    /// `canonical` sql string
    fn one_statement_parses_to(sql: &str, canonical: &str) -> Statement {
        let mut statements = DFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        if sql != canonical {
            assert_eq!(DFParser::parse_sql(canonical).unwrap(), statements);
        }

        let only_statement = statements.pop_front().unwrap();
        assert_eq!(
            canonical.to_uppercase(),
            only_statement.to_string().to_uppercase()
        );
        only_statement
    }

    /// Ensures that `sql` parses as a single [Statement], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    fn verified_stmt(sql: &str) -> Statement {
        one_statement_parses_to(sql, sql)
    }
}
