#![allow(non_snake_case)]
#![allow(dead_code)]

use antlr_rust::tree::ParseTree;
use datafusion_expr::{
    EmptyRelation, Expr, LogicalPlan, LogicalPlanBuilder, TableSource,
};

use crate::antlr::presto::prestoparser::*;
use datafusion_common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, OwnedTableReference, Result,
    TableReference,
};
use std::{cell::{RefCell, Cell}, rc::Rc, sync::Arc};

trait BindingContext {
    fn resolve_table(&self, _: TableReference) -> Result<Arc<dyn TableSource>> {
        Err(DataFusionError::NotImplemented(String::from(
            "Not implement resolve_table",
        )))
    }

    fn resolve_column(&self, _: String) -> Result<Expr> {
        Err(DataFusionError::NotImplemented(String::from(
            "Not implement resolve_column",
        )))
    }
}

struct ColumnBindingContext {
    schema: DFSchemaRef,
}

impl BindingContext for ColumnBindingContext {
    fn resolve_column(&self, name: String) -> Result<Expr> {
        match self.schema.index_of_column_by_name(None, name.as_str()) {
            Ok(_) => Ok(Expr::Column(Column {
                relation: None,
                name: name,
            })),
            Err(e) => Err(e),
        }
    }
}

struct BindingContextStack {
    stack: Cell<Vec<Arc<dyn BindingContext>>>,
}

impl BindingContextStack {
    fn new(stack: Cell<Vec<Arc<dyn BindingContext>>>) -> Self {
        BindingContextStack {
            stack: stack,
        }
    }

    fn push(&self, bc: Arc<dyn BindingContext>) -> BindingContextStack {
        let mut new_stack = self.stack.take().clone();
        new_stack.push(bc);
        BindingContextStack { stack: Cell::new(new_stack) }
    }
}

impl BindingContext for BindingContextStack {
    fn resolve_table(&self, table_ref: TableReference) -> Result<Arc<dyn TableSource>> {
        for bc in self.stack.take().iter().rev() {
            let result = bc.resolve_table(table_ref);
            if result.is_ok() {
                return result;
            }
        }
        Err(DataFusionError::Plan(format!(
            "No table named: {} found",
            table_ref.table()
        )))
    }
}

struct Binder {
    context: RefCell<BindingContextStack>,
}

impl Binder {
    fn new(context: RefCell<BindingContextStack>) -> Self {
        Binder {
            context: context,
        }
    }

    fn bind_LogicalPlan_from_singleStatement<'input>(
        &self,
        ctx: Rc<SingleStatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_statement(ctx.statement().unwrap())
    }

    fn bind_LogicalPlan_from_statement<'input>(
        &self,
        ctx: Rc<StatementContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            StatementContextAll::StatementDefaultContext(c) => {
                self.bind_LogicalPlan_from_statementDefault(c)
            }
            // StatmentContextAll::Use
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_statement",
            ))),
        }
    }

    fn bind_LogicalPlan_from_statementDefault<'input>(
        &self,
        ctx: &StatementDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_query(ctx.query().unwrap())
    }

    fn bind_LogicalPlan_from_query<'input>(
        &self,
        ctx: Rc<QueryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.with().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_query",
            )));
        }
        self.bind_LogicalPlan_from_queryNoWith(ctx.queryNoWith().unwrap())
    }

    fn bind_LogicalPlan_from_queryNoWith<'input>(
        &self,
        ctx: Rc<QueryNoWithContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.sortItem_all().len() > 0 {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented sortItem",
            )));
        }
        if ctx.offset.is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented offset",
            )));
        }
        if ctx.limit.is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented limit",
            )));
        }
        if ctx.FETCH().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented FETCH",
            )));
        }
        self.bind_LogicalPlan_from_queryTerm(ctx.queryTerm().unwrap())
    }

    fn bind_LogicalPlan_from_queryTerm<'input>(
        &self,
        ctx: Rc<QueryTermContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            QueryTermContextAll::QueryTermDefaultContext(c) => {
                self.bind_LogicalPlan_from_queryTermDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_queryTerm",
            ))),
        }
    }

    fn bind_LogicalPlan_from_queryTermDefault<'input>(
        &self,
        ctx: &QueryTermDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_queryPrimary(ctx.queryPrimary().unwrap())
    }

    fn bind_LogicalPlan_from_queryPrimary<'input>(
        &self,
        ctx: Rc<QueryPrimaryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            QueryPrimaryContextAll::QueryPrimaryDefaultContext(c) => {
                self.bind_LogicalPlan_from_queryPrimaryDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_queryPrimary",
            ))),
        }
    }

    fn bind_LogicalPlan_from_queryPrimaryDefault<'input>(
        &self,
        ctx: &QueryPrimaryDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_querySpecification(ctx.querySpecification().unwrap())
    }

    fn bind_LogicalPlan_from_querySpecification<'input>(
        &self,
        ctx: Rc<QuerySpecificationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.setQuantifier().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented setQuantifier",
            )));
        }
        if ctx.where_.is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented where",
            )));
        }
        if ctx.groupBy().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented groupby",
            )));
        }
        if ctx.having.is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented having",
            )));
        }
        if ctx.windowDefinition_all().len() > 0 {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented windowDefinition",
            )));
        }
        if ctx.relation_all().len() > 1 {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented relation",
            )));
        }
        let parent = if ctx.relation_all().len() == 0 {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: DFSchemaRef::new(DFSchema::empty()),
            })
        } else {
            self.bind_LogicalPlan_from_relation(ctx.relation(0).unwrap())?
        };

        let column_binding_context = ColumnBindingContext {
            schema: parent.schema().clone(),
        };

        let new_context = self.context.borrow().push(Arc::new(column_binding_context));
        let new_binder = Binder::new(RefCell::new(new_context));
        let items: Vec<_> = ctx
            .querySelectItems()
            .unwrap()
            .selectItem_all()
            .iter()
            .map(|item| new_binder.bind_Expr_from_selectItem(item.clone()).unwrap())
            .collect();

        LogicalPlanBuilder::from(parent).project(items)?.build()
    }

    fn bind_Expr_from_selectItem<'input>(
        &self,
        ctx: Rc<SelectItemContextAll<'input>>,
    ) -> Result<Expr> {
        Ok(Expr::Column(Column {
            relation: None,
            name: String::from("ID"),
        }))
    }

    fn bind_LogicalPlan_from_relation<'input>(
        &self,
        ctx: Rc<RelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            RelationContextAll::RelationDefaultContext(c) => {
                self.bind_LogicalPlan_from_relationDefault(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_relation",
            ))),
        }
    }

    fn bind_LogicalPlan_from_relationDefault<'input>(
        &self,
        ctx: &RelationDefaultContext<'input>,
    ) -> Result<LogicalPlan> {
        self.bind_LogicalPlan_from_sampledRelation(ctx.sampledRelation().unwrap())
    }

    fn bind_LogicalPlan_from_sampledRelation<'input>(
        &self,
        ctx: Rc<SampledRelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.sampleType().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented sampleType",
            )));
        }
        self.bind_LogicalPlan_from_patternRecognition(ctx.patternRecognition().unwrap())
    }

    fn bind_LogicalPlan_from_patternRecognition<'input>(
        &self,
        ctx: Rc<PatternRecognitionContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.MATCH_RECOGNIZE().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented MATCH_RECOGNIZE",
            )));
        }
        self.bind_LogicalPlan_from_aliasedRelation(ctx.aliasedRelation().unwrap())
    }

    fn bind_LogicalPlan_from_aliasedRelation<'input>(
        &self,
        ctx: Rc<AliasedRelationContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        if ctx.identifier().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented identifier in aliasedRelation",
            )));
        }
        self.bind_LogicalPlan_from_relationPrimary(ctx.relationPrimary().unwrap())
    }

    fn bind_LogicalPlan_from_relationPrimary<'input>(
        &self,
        ctx: Rc<RelationPrimaryContextAll<'input>>,
    ) -> Result<LogicalPlan> {
        match &*ctx {
            RelationPrimaryContextAll::TableNameContext(c) => {
                self.bind_LogicalPlan_from_tableName(c)
            }
            _ => Err(DataFusionError::NotImplemented(String::from(
                "not implemented bind_LogicalPlan_from_relationPrimary",
            ))),
        }
    }

    fn bind_LogicalPlan_from_tableName<'input>(
        &self,
        ctx: &TableNameContext<'input>,
    ) -> Result<LogicalPlan> {
        if ctx.queryPeriod().is_some() {
            return Err(DataFusionError::NotImplemented(String::from(
                "not implemented queryPeriod",
            )));
        }

        let table_ref_result = self
            .bind_OwnedTableReference_from_qualified_name(ctx.qualifiedName().unwrap());
        if table_ref_result.is_err() {
            return Err(table_ref_result.unwrap_err());
        }
        match self
            .context
            .borrow()
            .resolve_table(table_ref_result.unwrap().as_table_reference())
        {
            Ok(table_source) => {
                LogicalPlanBuilder::scan(String::from("PERSON"), table_source, None)?
                    .build()
            }
            Err(e) => Err(e),
        }
    }

    fn bind_OwnedTableReference_from_qualified_name<'input>(
        &self,
        ctx: Rc<QualifiedNameContextAll<'input>>,
    ) -> Result<OwnedTableReference> {
        let identifiers: Vec<_> = ctx
            .identifier_all()
            .iter()
            .map(|i| self.bind_str_from_identifier(i))
            .collect();
        if identifiers.len() == 1 {
            Ok(OwnedTableReference::Bare {
                table: identifiers[0].clone(),
            })
        } else if identifiers.len() == 2 {
            Ok(OwnedTableReference::Partial {
                schema: identifiers[0].clone(),
                table: identifiers[1].clone(),
            })
        } else {
            Err(DataFusionError::Plan(
                "Cannot bind TableReference".to_owned(),
            ))
        }
    }

    fn bind_str_from_identifier<'input>(
        &self,
        ctx: &Rc<IdentifierContextAll<'input>>,
    ) -> String {
        ctx.get_text()
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{RefCell, Cell};
    use std::rc::Rc;
    use std::result;
    use std::sync::Arc;

    use crate::antlr::presto::prestolexer::PrestoLexer;
    use crate::antlr::presto::prestoparser::{PrestoParser, SingleStatementContextAll};
    use crate::planner2::{Binder, BindingContextStack};
    use antlr_rust::common_token_stream::CommonTokenStream;
    use antlr_rust::errors::ANTLRError;
    use antlr_rust::input_stream::InputStream;
    use antlr_rust::token_factory::ArenaCommonFactory;
    use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion_common::Result;
    use datafusion_common::{DataFusionError, TableReference};
    use datafusion_expr::TableSource;

    use super::BindingContext;

    fn parse<'input>(
        sql: &'input str,
        tf: &'input ArenaCommonFactory<'input>,
    ) -> result::Result<Rc<SingleStatementContextAll<'input>>, ANTLRError> {
        println!("test started");

        let mut _lexer: PrestoLexer<'input, InputStream<&'input str>> =
            PrestoLexer::new_with_token_factory(InputStream::new(&sql), &tf);
        let token_source = CommonTokenStream::new(_lexer);
        let mut parser = PrestoParser::new(token_source);
        println!("\nstart parsing");
        parser.singleStatement()
    }

    struct EmptyTable {
        table_schema: SchemaRef,
    }

    impl EmptyTable {
        fn new(table_schema: SchemaRef) -> Self {
            Self { table_schema }
        }
    }

    impl TableSource for EmptyTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            self.table_schema.clone()
        }
    }

    struct TableBindingContext;

    impl TableBindingContext {
        fn new() -> Self {
            TableBindingContext
        }
    }

    impl BindingContext for TableBindingContext {
        fn resolve_table(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
            let schema = match name.table() {
                "PERSON" => Ok(Schema::new(vec![
                    Field::new("ID", DataType::UInt32, false),
                    Field::new("first_name", DataType::Utf8, false),
                    Field::new("last_name", DataType::Utf8, false),
                    Field::new("age", DataType::Int32, false),
                    Field::new("state", DataType::Utf8, false),
                    Field::new("salary", DataType::Float64, false),
                    Field::new(
                        "birth_date",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    ),
                    Field::new("😀", DataType::Int32, false),
                ])),
                _ => Err(DataFusionError::Plan(format!(
                    "No table named: {} found",
                    name.table()
                ))),
            };

            match schema {
                Ok(t) => Ok(Arc::new(EmptyTable::new(Arc::new(t)))),
                Err(e) => Err(e),
            }
        }
    }
    #[test]
    fn it_works() {
        let tf = ArenaCommonFactory::default();
        let root = parse("SELECT ID FROM PERSON", &tf).unwrap();
        let binder = Binder::new(RefCell::new(BindingContextStack::new(Cell::new(vec![Arc::new(TableBindingContext::new())]))));
        
        let plan = binder.bind_LogicalPlan_from_singleStatement(root).unwrap();
        let expected = "Projection: PERSON.ID\n  TableScan: PERSON";
        assert_eq!(expected, format!("{plan:?}"));
    }
}
