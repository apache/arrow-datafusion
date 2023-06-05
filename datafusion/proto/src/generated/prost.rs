#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnRelation {
    #[prost(string, tag = "1")]
    pub relation: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Column {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub relation: ::core::option::Option<ColumnRelation>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfField {
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
    #[prost(message, optional, tag = "2")]
    pub qualifier: ::core::option::Option<ColumnRelation>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DfSchema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<DfField>,
    #[prost(map = "string, string", tag = "2")]
    pub metadata: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
/// logical plan
/// LogicalPlan is a nested type
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalPlanNode {
    #[prost(
        oneof = "logical_plan_node::LogicalPlanType",
        tags = "1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27"
    )]
    pub logical_plan_type: ::core::option::Option<logical_plan_node::LogicalPlanType>,
}
/// Nested message and enum types in `LogicalPlanNode`.
pub mod logical_plan_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum LogicalPlanType {
        #[prost(message, tag = "1")]
        ListingScan(super::ListingTableScanNode),
        #[prost(message, tag = "3")]
        Projection(::prost::alloc::boxed::Box<super::ProjectionNode>),
        #[prost(message, tag = "4")]
        Selection(::prost::alloc::boxed::Box<super::SelectionNode>),
        #[prost(message, tag = "5")]
        Limit(::prost::alloc::boxed::Box<super::LimitNode>),
        #[prost(message, tag = "6")]
        Aggregate(::prost::alloc::boxed::Box<super::AggregateNode>),
        #[prost(message, tag = "7")]
        Join(::prost::alloc::boxed::Box<super::JoinNode>),
        #[prost(message, tag = "8")]
        Sort(::prost::alloc::boxed::Box<super::SortNode>),
        #[prost(message, tag = "9")]
        Repartition(::prost::alloc::boxed::Box<super::RepartitionNode>),
        #[prost(message, tag = "10")]
        EmptyRelation(super::EmptyRelationNode),
        #[prost(message, tag = "11")]
        CreateExternalTable(super::CreateExternalTableNode),
        #[prost(message, tag = "12")]
        Explain(::prost::alloc::boxed::Box<super::ExplainNode>),
        #[prost(message, tag = "13")]
        Window(::prost::alloc::boxed::Box<super::WindowNode>),
        #[prost(message, tag = "14")]
        Analyze(::prost::alloc::boxed::Box<super::AnalyzeNode>),
        #[prost(message, tag = "15")]
        CrossJoin(::prost::alloc::boxed::Box<super::CrossJoinNode>),
        #[prost(message, tag = "16")]
        Values(super::ValuesNode),
        #[prost(message, tag = "17")]
        Extension(super::LogicalExtensionNode),
        #[prost(message, tag = "18")]
        CreateCatalogSchema(super::CreateCatalogSchemaNode),
        #[prost(message, tag = "19")]
        Union(super::UnionNode),
        #[prost(message, tag = "20")]
        CreateCatalog(super::CreateCatalogNode),
        #[prost(message, tag = "21")]
        SubqueryAlias(::prost::alloc::boxed::Box<super::SubqueryAliasNode>),
        #[prost(message, tag = "22")]
        CreateView(::prost::alloc::boxed::Box<super::CreateViewNode>),
        #[prost(message, tag = "23")]
        Distinct(::prost::alloc::boxed::Box<super::DistinctNode>),
        #[prost(message, tag = "24")]
        ViewScan(::prost::alloc::boxed::Box<super::ViewTableScanNode>),
        #[prost(message, tag = "25")]
        CustomScan(super::CustomTableScanNode),
        #[prost(message, tag = "26")]
        Prepare(::prost::alloc::boxed::Box<super::PrepareNode>),
        #[prost(message, tag = "27")]
        DropView(super::DropViewNode),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExtensionNode {
    #[prost(bytes = "vec", tag = "1")]
    pub node: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub inputs: ::prost::alloc::vec::Vec<LogicalPlanNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionColumns {
    #[prost(string, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CsvFormat {
    #[prost(bool, tag = "1")]
    pub has_header: bool,
    #[prost(string, tag = "2")]
    pub delimiter: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetFormat {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvroFormat {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprNodeCollection {
    #[prost(message, repeated, tag = "1")]
    pub logical_expr_nodes: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListingTableScanNode {
    #[prost(message, optional, tag = "14")]
    pub table_name: ::core::option::Option<OwnedTableReference>,
    #[prost(string, repeated, tag = "2")]
    pub paths: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "3")]
    pub file_extension: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag = "5")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "6")]
    pub filters: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(string, repeated, tag = "7")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "8")]
    pub collect_stat: bool,
    #[prost(uint32, tag = "9")]
    pub target_partitions: u32,
    #[prost(message, repeated, tag = "13")]
    pub file_sort_order: ::prost::alloc::vec::Vec<LogicalExprNodeCollection>,
    #[prost(oneof = "listing_table_scan_node::FileFormatType", tags = "10, 11, 12")]
    pub file_format_type: ::core::option::Option<
        listing_table_scan_node::FileFormatType,
    >,
}
/// Nested message and enum types in `ListingTableScanNode`.
pub mod listing_table_scan_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FileFormatType {
        #[prost(message, tag = "10")]
        Csv(super::CsvFormat),
        #[prost(message, tag = "11")]
        Parquet(super::ParquetFormat),
        #[prost(message, tag = "12")]
        Avro(super::AvroFormat),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ViewTableScanNode {
    #[prost(message, optional, tag = "6")]
    pub table_name: ::core::option::Option<OwnedTableReference>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, optional, tag = "4")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(string, tag = "5")]
    pub definition: ::prost::alloc::string::String,
}
/// Logical Plan to Scan a CustomTableProvider registered at runtime
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CustomTableScanNode {
    #[prost(message, optional, tag = "6")]
    pub table_name: ::core::option::Option<OwnedTableReference>,
    #[prost(message, optional, tag = "2")]
    pub projection: ::core::option::Option<ProjectionColumns>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "4")]
    pub filters: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bytes = "vec", tag = "5")]
    pub custom_table_data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(oneof = "projection_node::OptionalAlias", tags = "3")]
    pub optional_alias: ::core::option::Option<projection_node::OptionalAlias>,
}
/// Nested message and enum types in `ProjectionNode`.
pub mod projection_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum OptionalAlias {
        #[prost(string, tag = "3")]
        Alias(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::core::option::Option<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    /// Maximum number of highest/lowest rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepartitionNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(oneof = "repartition_node::PartitionMethod", tags = "2, 3")]
    pub partition_method: ::core::option::Option<repartition_node::PartitionMethod>,
}
/// Nested message and enum types in `RepartitionNode`.
pub mod repartition_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PartitionMethod {
        #[prost(uint64, tag = "2")]
        RoundRobin(u64),
        #[prost(message, tag = "3")]
        Hash(super::HashRepartition),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashRepartition {
    #[prost(message, repeated, tag = "1")]
    pub hash_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(uint64, tag = "2")]
    pub partition_count: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyRelationNode {
    #[prost(bool, tag = "1")]
    pub produce_one_row: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateExternalTableNode {
    #[prost(message, optional, tag = "12")]
    pub name: ::core::option::Option<OwnedTableReference>,
    #[prost(string, tag = "2")]
    pub location: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub file_type: ::prost::alloc::string::String,
    #[prost(bool, tag = "4")]
    pub has_header: bool,
    #[prost(message, optional, tag = "5")]
    pub schema: ::core::option::Option<DfSchema>,
    #[prost(string, repeated, tag = "6")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(bool, tag = "7")]
    pub if_not_exists: bool,
    #[prost(string, tag = "8")]
    pub delimiter: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub definition: ::prost::alloc::string::String,
    #[prost(string, tag = "10")]
    pub file_compression_type: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "13")]
    pub order_exprs: ::prost::alloc::vec::Vec<LogicalExprNodeCollection>,
    #[prost(bool, tag = "14")]
    pub unbounded: bool,
    #[prost(map = "string, string", tag = "11")]
    pub options: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PrepareNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub data_types: ::prost::alloc::vec::Vec<ArrowType>,
    #[prost(message, optional, boxed, tag = "3")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogSchemaNode {
    #[prost(string, tag = "1")]
    pub schema_name: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<DfSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateCatalogNode {
    #[prost(string, tag = "1")]
    pub catalog_name: ::prost::alloc::string::String,
    #[prost(bool, tag = "2")]
    pub if_not_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<DfSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DropViewNode {
    #[prost(message, optional, tag = "1")]
    pub name: ::core::option::Option<OwnedTableReference>,
    #[prost(bool, tag = "2")]
    pub if_exists: bool,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<DfSchema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateViewNode {
    #[prost(message, optional, tag = "5")]
    pub name: ::core::option::Option<OwnedTableReference>,
    #[prost(message, optional, boxed, tag = "2")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "3")]
    pub or_replace: bool,
    #[prost(string, tag = "4")]
    pub definition: ::prost::alloc::string::String,
}
/// a node containing data for defining values list. unlike in SQL where it's two dimensional, here
/// the list is flattened, and with the field n_cols it can be parsed and partitioned into rows
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValuesNode {
    #[prost(uint64, tag = "1")]
    pub n_cols: u64,
    #[prost(message, repeated, tag = "2")]
    pub values_list: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "2")]
    pub verbose: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(bool, tag = "2")]
    pub verbose: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub group_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag = "3")]
    pub aggr_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub window_expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(enumeration = "JoinType", tag = "3")]
    pub join_type: i32,
    #[prost(enumeration = "JoinConstraint", tag = "4")]
    pub join_constraint: i32,
    #[prost(message, repeated, tag = "5")]
    pub left_join_key: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag = "6")]
    pub right_join_key: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag = "7")]
    pub null_equals_null: bool,
    #[prost(message, optional, tag = "8")]
    pub filter: ::core::option::Option<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DistinctNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnionNode {
    #[prost(message, repeated, tag = "1")]
    pub inputs: ::prost::alloc::vec::Vec<LogicalPlanNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrossJoinNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LimitNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    /// The number of rows to skip before fetch; non-positive means don't skip any
    #[prost(int64, tag = "2")]
    pub skip: i64,
    /// Maximum number of rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SelectionExecNode {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SubqueryAliasNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<LogicalPlanNode>>,
    #[prost(message, optional, tag = "3")]
    pub alias: ::core::option::Option<OwnedTableReference>,
}
/// logical expressions
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprNode {
    #[prost(
        oneof = "logical_expr_node::ExprType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34"
    )]
    pub expr_type: ::core::option::Option<logical_expr_node::ExprType>,
}
/// Nested message and enum types in `LogicalExprNode`.
pub mod logical_expr_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprType {
        /// column references
        #[prost(message, tag = "1")]
        Column(super::Column),
        /// alias
        #[prost(message, tag = "2")]
        Alias(::prost::alloc::boxed::Box<super::AliasNode>),
        #[prost(message, tag = "3")]
        Literal(super::ScalarValue),
        /// binary expressions
        #[prost(message, tag = "4")]
        BinaryExpr(super::BinaryExprNode),
        /// aggregate expressions
        #[prost(message, tag = "5")]
        AggregateExpr(::prost::alloc::boxed::Box<super::AggregateExprNode>),
        /// null checks
        #[prost(message, tag = "6")]
        IsNullExpr(::prost::alloc::boxed::Box<super::IsNull>),
        #[prost(message, tag = "7")]
        IsNotNullExpr(::prost::alloc::boxed::Box<super::IsNotNull>),
        #[prost(message, tag = "8")]
        NotExpr(::prost::alloc::boxed::Box<super::Not>),
        #[prost(message, tag = "9")]
        Between(::prost::alloc::boxed::Box<super::BetweenNode>),
        #[prost(message, tag = "10")]
        Case(::prost::alloc::boxed::Box<super::CaseNode>),
        #[prost(message, tag = "11")]
        Cast(::prost::alloc::boxed::Box<super::CastNode>),
        #[prost(message, tag = "12")]
        Sort(::prost::alloc::boxed::Box<super::SortExprNode>),
        #[prost(message, tag = "13")]
        Negative(::prost::alloc::boxed::Box<super::NegativeNode>),
        #[prost(message, tag = "14")]
        InList(::prost::alloc::boxed::Box<super::InListNode>),
        #[prost(bool, tag = "15")]
        Wildcard(bool),
        #[prost(message, tag = "16")]
        ScalarFunction(super::ScalarFunctionNode),
        #[prost(message, tag = "17")]
        TryCast(::prost::alloc::boxed::Box<super::TryCastNode>),
        /// window expressions
        #[prost(message, tag = "18")]
        WindowExpr(::prost::alloc::boxed::Box<super::WindowExprNode>),
        /// AggregateUDF expressions
        #[prost(message, tag = "19")]
        AggregateUdfExpr(::prost::alloc::boxed::Box<super::AggregateUdfExprNode>),
        /// Scalar UDF expressions
        #[prost(message, tag = "20")]
        ScalarUdfExpr(super::ScalarUdfExprNode),
        #[prost(message, tag = "21")]
        GetIndexedField(::prost::alloc::boxed::Box<super::GetIndexedField>),
        #[prost(message, tag = "22")]
        GroupingSet(super::GroupingSetNode),
        #[prost(message, tag = "23")]
        Cube(super::CubeNode),
        #[prost(message, tag = "24")]
        Rollup(super::RollupNode),
        #[prost(message, tag = "25")]
        IsTrue(::prost::alloc::boxed::Box<super::IsTrue>),
        #[prost(message, tag = "26")]
        IsFalse(::prost::alloc::boxed::Box<super::IsFalse>),
        #[prost(message, tag = "27")]
        IsUnknown(::prost::alloc::boxed::Box<super::IsUnknown>),
        #[prost(message, tag = "28")]
        IsNotTrue(::prost::alloc::boxed::Box<super::IsNotTrue>),
        #[prost(message, tag = "29")]
        IsNotFalse(::prost::alloc::boxed::Box<super::IsNotFalse>),
        #[prost(message, tag = "30")]
        IsNotUnknown(::prost::alloc::boxed::Box<super::IsNotUnknown>),
        #[prost(message, tag = "31")]
        Like(::prost::alloc::boxed::Box<super::LikeNode>),
        #[prost(message, tag = "32")]
        Ilike(::prost::alloc::boxed::Box<super::ILikeNode>),
        #[prost(message, tag = "33")]
        SimilarTo(::prost::alloc::boxed::Box<super::SimilarToNode>),
        #[prost(message, tag = "34")]
        Placeholder(super::PlaceholderNode),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PlaceholderNode {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub data_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogicalExprList {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GroupingSetNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprList>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CubeNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RollupNode {
    #[prost(message, repeated, tag = "1")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetIndexedField {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub key: ::core::option::Option<ScalarValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsTrue {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsFalse {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsUnknown {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotTrue {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotFalse {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNotUnknown {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Not {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AliasNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BinaryExprNode {
    /// Represents the operands from the left inner most expression
    /// to the right outer most expression where each of them are chained
    /// with the operator 'op'.
    #[prost(message, repeated, tag = "1")]
    pub operands: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NegativeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InListNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub list: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag = "3")]
    pub negated: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarFunctionNode {
    #[prost(enumeration = "ScalarFunction", tag = "1")]
    pub fun: i32,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateExprNode {
    #[prost(enumeration = "AggregateFunction", tag = "1")]
    pub aggr_function: i32,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(bool, tag = "3")]
    pub distinct: bool,
    #[prost(message, optional, boxed, tag = "4")]
    pub filter: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "5")]
    pub order_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateUdfExprNode {
    #[prost(string, tag = "1")]
    pub fun_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, optional, boxed, tag = "3")]
    pub filter: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "4")]
    pub order_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarUdfExprNode {
    #[prost(string, tag = "1")]
    pub fun_name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowExprNode {
    #[prost(message, optional, boxed, tag = "4")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "5")]
    pub partition_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
    #[prost(message, repeated, tag = "6")]
    pub order_by: ::prost::alloc::vec::Vec<LogicalExprNode>,
    /// repeated LogicalExprNode filter = 7;
    #[prost(message, optional, tag = "8")]
    pub window_frame: ::core::option::Option<WindowFrame>,
    #[prost(oneof = "window_expr_node::WindowFunction", tags = "1, 2")]
    pub window_function: ::core::option::Option<window_expr_node::WindowFunction>,
}
/// Nested message and enum types in `WindowExprNode`.
pub mod window_expr_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WindowFunction {
        #[prost(enumeration = "super::AggregateFunction", tag = "1")]
        AggrFunction(i32),
        /// udaf = 3
        #[prost(enumeration = "super::BuiltInWindowFunction", tag = "2")]
        BuiltInFunction(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BetweenNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "3")]
    pub low: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub high: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LikeNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ILikeNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SimilarToNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(message, optional, boxed, tag = "2")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, boxed, tag = "3")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(string, tag = "4")]
    pub escape_char: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CaseNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub when_then_expr: ::prost::alloc::vec::Vec<WhenThen>,
    #[prost(message, optional, boxed, tag = "3")]
    pub else_expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhenThen {
    #[prost(message, optional, tag = "1")]
    pub when_expr: ::core::option::Option<LogicalExprNode>,
    #[prost(message, optional, tag = "2")]
    pub then_expr: ::core::option::Option<LogicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TryCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<LogicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub asc: bool,
    #[prost(bool, tag = "3")]
    pub nulls_first: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrame {
    #[prost(enumeration = "WindowFrameUnits", tag = "1")]
    pub window_frame_units: i32,
    #[prost(message, optional, tag = "2")]
    pub start_bound: ::core::option::Option<WindowFrameBound>,
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[prost(oneof = "window_frame::EndBound", tags = "3")]
    pub end_bound: ::core::option::Option<window_frame::EndBound>,
}
/// Nested message and enum types in `WindowFrame`.
pub mod window_frame {
    /// "optional" keyword is stable in protoc 3.15 but prost is still on 3.14 (see <https://github.com/tokio-rs/prost/issues/430> and <https://github.com/tokio-rs/prost/pull/455>)
    /// this syntax is ugly but is binary compatible with the "optional" keyword (see <https://stackoverflow.com/questions/42622015/how-to-define-an-optional-field-in-protobuf-3>)
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum EndBound {
        #[prost(message, tag = "3")]
        Bound(super::WindowFrameBound),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowFrameBound {
    #[prost(enumeration = "WindowFrameBoundType", tag = "1")]
    pub window_frame_bound_type: i32,
    #[prost(message, optional, tag = "2")]
    pub bound_value: ::core::option::Option<ScalarValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(message, repeated, tag = "1")]
    pub columns: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    /// name of the field
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag = "2")]
    pub arrow_type: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(bool, tag = "3")]
    pub nullable: bool,
    /// for complex data types like structs, unions
    #[prost(message, repeated, tag = "4")]
    pub children: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeBinary {
    #[prost(int32, tag = "1")]
    pub length: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(enumeration = "TimeUnit", tag = "1")]
    pub time_unit: i32,
    #[prost(string, tag = "2")]
    pub timezone: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal {
    #[prost(uint32, tag = "3")]
    pub precision: u32,
    #[prost(int32, tag = "4")]
    pub scale: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct List {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FixedSizeList {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
    #[prost(int32, tag = "2")]
    pub list_size: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Dictionary {
    #[prost(message, optional, boxed, tag = "1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ArrowType>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Struct {
    #[prost(message, repeated, tag = "1")]
    pub sub_field_types: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Map {
    #[prost(message, optional, boxed, tag = "1")]
    pub field_type: ::core::option::Option<::prost::alloc::boxed::Box<Field>>,
    #[prost(bool, tag = "2")]
    pub keys_sorted: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Union {
    #[prost(message, repeated, tag = "1")]
    pub union_types: ::prost::alloc::vec::Vec<Field>,
    #[prost(enumeration = "UnionMode", tag = "2")]
    pub union_mode: i32,
    #[prost(int32, repeated, tag = "3")]
    pub type_ids: ::prost::alloc::vec::Vec<i32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarListValue {
    /// encode null explicitly to distinguish a list with a null value
    /// from a list with no values)
    #[prost(bool, tag = "3")]
    pub is_null: bool,
    #[prost(message, optional, tag = "1")]
    pub field: ::core::option::Option<Field>,
    #[prost(message, repeated, tag = "2")]
    pub values: ::prost::alloc::vec::Vec<ScalarValue>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime32Value {
    #[prost(oneof = "scalar_time32_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time32_value::Value>,
}
/// Nested message and enum types in `ScalarTime32Value`.
pub mod scalar_time32_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int32, tag = "1")]
        Time32SecondValue(i32),
        #[prost(int32, tag = "2")]
        Time32MillisecondValue(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTime64Value {
    #[prost(oneof = "scalar_time64_value::Value", tags = "1, 2")]
    pub value: ::core::option::Option<scalar_time64_value::Value>,
}
/// Nested message and enum types in `ScalarTime64Value`.
pub mod scalar_time64_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        Time64MicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        Time64NanosecondValue(i64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarTimestampValue {
    #[prost(string, tag = "5")]
    pub timezone: ::prost::alloc::string::String,
    #[prost(oneof = "scalar_timestamp_value::Value", tags = "1, 2, 3, 4")]
    pub value: ::core::option::Option<scalar_timestamp_value::Value>,
}
/// Nested message and enum types in `ScalarTimestampValue`.
pub mod scalar_timestamp_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(int64, tag = "1")]
        TimeMicrosecondValue(i64),
        #[prost(int64, tag = "2")]
        TimeNanosecondValue(i64),
        #[prost(int64, tag = "3")]
        TimeSecondValue(i64),
        #[prost(int64, tag = "4")]
        TimeMillisecondValue(i64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarDictionaryValue {
    #[prost(message, optional, tag = "1")]
    pub index_type: ::core::option::Option<ArrowType>,
    #[prost(message, optional, boxed, tag = "2")]
    pub value: ::core::option::Option<::prost::alloc::boxed::Box<ScalarValue>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntervalMonthDayNanoValue {
    #[prost(int32, tag = "1")]
    pub months: i32,
    #[prost(int32, tag = "2")]
    pub days: i32,
    #[prost(int64, tag = "3")]
    pub nanos: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructValue {
    /// Note that a null struct value must have one or more fields, so we
    /// encode a null StructValue as one witth an empty field_values
    /// list.
    #[prost(message, repeated, tag = "2")]
    pub field_values: ::prost::alloc::vec::Vec<ScalarValue>,
    #[prost(message, repeated, tag = "3")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarFixedSizeBinary {
    #[prost(bytes = "vec", tag = "1")]
    pub values: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "2")]
    pub length: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarValue {
    #[prost(
        oneof = "scalar_value::Value",
        tags = "33, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17, 20, 21, 24, 25, 26, 27, 28, 29, 30, 31, 32, 34"
    )]
    pub value: ::core::option::Option<scalar_value::Value>,
}
/// Nested message and enum types in `ScalarValue`.
pub mod scalar_value {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        /// was PrimitiveScalarType null_value = 19;
        /// Null value of any type
        #[prost(message, tag = "33")]
        NullValue(super::ArrowType),
        #[prost(bool, tag = "1")]
        BoolValue(bool),
        #[prost(string, tag = "2")]
        Utf8Value(::prost::alloc::string::String),
        #[prost(string, tag = "3")]
        LargeUtf8Value(::prost::alloc::string::String),
        #[prost(int32, tag = "4")]
        Int8Value(i32),
        #[prost(int32, tag = "5")]
        Int16Value(i32),
        #[prost(int32, tag = "6")]
        Int32Value(i32),
        #[prost(int64, tag = "7")]
        Int64Value(i64),
        #[prost(uint32, tag = "8")]
        Uint8Value(u32),
        #[prost(uint32, tag = "9")]
        Uint16Value(u32),
        #[prost(uint32, tag = "10")]
        Uint32Value(u32),
        #[prost(uint64, tag = "11")]
        Uint64Value(u64),
        #[prost(float, tag = "12")]
        Float32Value(f32),
        #[prost(double, tag = "13")]
        Float64Value(f64),
        /// Literal Date32 value always has a unit of day
        #[prost(int32, tag = "14")]
        Date32Value(i32),
        #[prost(message, tag = "15")]
        Time32Value(super::ScalarTime32Value),
        /// WAS: ScalarType null_list_value = 18;
        #[prost(message, tag = "17")]
        ListValue(super::ScalarListValue),
        #[prost(message, tag = "20")]
        Decimal128Value(super::Decimal128),
        #[prost(int64, tag = "21")]
        Date64Value(i64),
        #[prost(int32, tag = "24")]
        IntervalYearmonthValue(i32),
        #[prost(int64, tag = "25")]
        IntervalDaytimeValue(i64),
        #[prost(message, tag = "26")]
        TimestampValue(super::ScalarTimestampValue),
        #[prost(message, tag = "27")]
        DictionaryValue(::prost::alloc::boxed::Box<super::ScalarDictionaryValue>),
        #[prost(bytes, tag = "28")]
        BinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "29")]
        LargeBinaryValue(::prost::alloc::vec::Vec<u8>),
        #[prost(message, tag = "30")]
        Time64Value(super::ScalarTime64Value),
        #[prost(message, tag = "31")]
        IntervalMonthDayNano(super::IntervalMonthDayNanoValue),
        #[prost(message, tag = "32")]
        StructValue(super::StructValue),
        #[prost(message, tag = "34")]
        FixedSizeBinaryValue(super::ScalarFixedSizeBinary),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Decimal128 {
    #[prost(bytes = "vec", tag = "1")]
    pub value: ::prost::alloc::vec::Vec<u8>,
    #[prost(int64, tag = "2")]
    pub p: i64,
    #[prost(int64, tag = "3")]
    pub s: i64,
}
/// Serialized data type
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowType {
    #[prost(
        oneof = "arrow_type::ArrowTypeEnum",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 32, 15, 16, 31, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 33"
    )]
    pub arrow_type_enum: ::core::option::Option<arrow_type::ArrowTypeEnum>,
}
/// Nested message and enum types in `ArrowType`.
pub mod arrow_type {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ArrowTypeEnum {
        /// arrow::Type::NA
        #[prost(message, tag = "1")]
        None(super::EmptyMessage),
        /// arrow::Type::BOOL
        #[prost(message, tag = "2")]
        Bool(super::EmptyMessage),
        /// arrow::Type::UINT8
        #[prost(message, tag = "3")]
        Uint8(super::EmptyMessage),
        /// arrow::Type::INT8
        #[prost(message, tag = "4")]
        Int8(super::EmptyMessage),
        /// represents arrow::Type fields in src/arrow/type.h
        #[prost(message, tag = "5")]
        Uint16(super::EmptyMessage),
        #[prost(message, tag = "6")]
        Int16(super::EmptyMessage),
        #[prost(message, tag = "7")]
        Uint32(super::EmptyMessage),
        #[prost(message, tag = "8")]
        Int32(super::EmptyMessage),
        #[prost(message, tag = "9")]
        Uint64(super::EmptyMessage),
        #[prost(message, tag = "10")]
        Int64(super::EmptyMessage),
        #[prost(message, tag = "11")]
        Float16(super::EmptyMessage),
        #[prost(message, tag = "12")]
        Float32(super::EmptyMessage),
        #[prost(message, tag = "13")]
        Float64(super::EmptyMessage),
        #[prost(message, tag = "14")]
        Utf8(super::EmptyMessage),
        #[prost(message, tag = "32")]
        LargeUtf8(super::EmptyMessage),
        #[prost(message, tag = "15")]
        Binary(super::EmptyMessage),
        #[prost(int32, tag = "16")]
        FixedSizeBinary(i32),
        #[prost(message, tag = "31")]
        LargeBinary(super::EmptyMessage),
        #[prost(message, tag = "17")]
        Date32(super::EmptyMessage),
        #[prost(message, tag = "18")]
        Date64(super::EmptyMessage),
        #[prost(enumeration = "super::TimeUnit", tag = "19")]
        Duration(i32),
        #[prost(message, tag = "20")]
        Timestamp(super::Timestamp),
        #[prost(enumeration = "super::TimeUnit", tag = "21")]
        Time32(i32),
        #[prost(enumeration = "super::TimeUnit", tag = "22")]
        Time64(i32),
        #[prost(enumeration = "super::IntervalUnit", tag = "23")]
        Interval(i32),
        #[prost(message, tag = "24")]
        Decimal(super::Decimal),
        #[prost(message, tag = "25")]
        List(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "26")]
        LargeList(::prost::alloc::boxed::Box<super::List>),
        #[prost(message, tag = "27")]
        FixedSizeList(::prost::alloc::boxed::Box<super::FixedSizeList>),
        #[prost(message, tag = "28")]
        Struct(super::Struct),
        #[prost(message, tag = "29")]
        Union(super::Union),
        #[prost(message, tag = "30")]
        Dictionary(::prost::alloc::boxed::Box<super::Dictionary>),
        #[prost(message, tag = "33")]
        Map(::prost::alloc::boxed::Box<super::Map>),
    }
}
/// Useful for representing an empty enum variant in rust
/// E.G. enum example{One, Two(i32)}
/// maps to
/// message example{
///     oneof{
///         EmptyMessage One = 1;
///         i32 Two = 2;
///    }
/// }
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyMessage {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnalyzedLogicalPlanType {
    #[prost(string, tag = "1")]
    pub analyzer_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizedLogicalPlanType {
    #[prost(string, tag = "1")]
    pub optimizer_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OptimizedPhysicalPlanType {
    #[prost(string, tag = "1")]
    pub optimizer_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PlanType {
    #[prost(oneof = "plan_type::PlanTypeEnum", tags = "1, 7, 8, 2, 3, 4, 5, 6")]
    pub plan_type_enum: ::core::option::Option<plan_type::PlanTypeEnum>,
}
/// Nested message and enum types in `PlanType`.
pub mod plan_type {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PlanTypeEnum {
        #[prost(message, tag = "1")]
        InitialLogicalPlan(super::EmptyMessage),
        #[prost(message, tag = "7")]
        AnalyzedLogicalPlan(super::AnalyzedLogicalPlanType),
        #[prost(message, tag = "8")]
        FinalAnalyzedLogicalPlan(super::EmptyMessage),
        #[prost(message, tag = "2")]
        OptimizedLogicalPlan(super::OptimizedLogicalPlanType),
        #[prost(message, tag = "3")]
        FinalLogicalPlan(super::EmptyMessage),
        #[prost(message, tag = "4")]
        InitialPhysicalPlan(super::EmptyMessage),
        #[prost(message, tag = "5")]
        OptimizedPhysicalPlan(super::OptimizedPhysicalPlanType),
        #[prost(message, tag = "6")]
        FinalPhysicalPlan(super::EmptyMessage),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringifiedPlan {
    #[prost(message, optional, tag = "1")]
    pub plan_type: ::core::option::Option<PlanType>,
    #[prost(string, tag = "2")]
    pub plan: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BareTableReference {
    #[prost(string, tag = "1")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartialTableReference {
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FullTableReference {
    #[prost(string, tag = "1")]
    pub catalog: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OwnedTableReference {
    #[prost(oneof = "owned_table_reference::TableReferenceEnum", tags = "1, 2, 3")]
    pub table_reference_enum: ::core::option::Option<
        owned_table_reference::TableReferenceEnum,
    >,
}
/// Nested message and enum types in `OwnedTableReference`.
pub mod owned_table_reference {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum TableReferenceEnum {
        #[prost(message, tag = "1")]
        Bare(super::BareTableReference),
        #[prost(message, tag = "2")]
        Partial(super::PartialTableReference),
        #[prost(message, tag = "3")]
        Full(super::FullTableReference),
    }
}
/// PhysicalPlanNode is a nested type
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalPlanNode {
    #[prost(
        oneof = "physical_plan_node::PhysicalPlanType",
        tags = "1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21"
    )]
    pub physical_plan_type: ::core::option::Option<physical_plan_node::PhysicalPlanType>,
}
/// Nested message and enum types in `PhysicalPlanNode`.
pub mod physical_plan_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PhysicalPlanType {
        #[prost(message, tag = "1")]
        ParquetScan(super::ParquetScanExecNode),
        #[prost(message, tag = "2")]
        CsvScan(super::CsvScanExecNode),
        #[prost(message, tag = "3")]
        Empty(super::EmptyExecNode),
        #[prost(message, tag = "4")]
        Projection(::prost::alloc::boxed::Box<super::ProjectionExecNode>),
        #[prost(message, tag = "6")]
        GlobalLimit(::prost::alloc::boxed::Box<super::GlobalLimitExecNode>),
        #[prost(message, tag = "7")]
        LocalLimit(::prost::alloc::boxed::Box<super::LocalLimitExecNode>),
        #[prost(message, tag = "8")]
        Aggregate(::prost::alloc::boxed::Box<super::AggregateExecNode>),
        #[prost(message, tag = "9")]
        HashJoin(::prost::alloc::boxed::Box<super::HashJoinExecNode>),
        #[prost(message, tag = "10")]
        Sort(::prost::alloc::boxed::Box<super::SortExecNode>),
        #[prost(message, tag = "11")]
        CoalesceBatches(::prost::alloc::boxed::Box<super::CoalesceBatchesExecNode>),
        #[prost(message, tag = "12")]
        Filter(::prost::alloc::boxed::Box<super::FilterExecNode>),
        #[prost(message, tag = "13")]
        Merge(::prost::alloc::boxed::Box<super::CoalescePartitionsExecNode>),
        #[prost(message, tag = "14")]
        Repartition(::prost::alloc::boxed::Box<super::RepartitionExecNode>),
        #[prost(message, tag = "15")]
        Window(::prost::alloc::boxed::Box<super::WindowAggExecNode>),
        #[prost(message, tag = "16")]
        CrossJoin(::prost::alloc::boxed::Box<super::CrossJoinExecNode>),
        #[prost(message, tag = "17")]
        AvroScan(super::AvroScanExecNode),
        #[prost(message, tag = "18")]
        Extension(super::PhysicalExtensionNode),
        #[prost(message, tag = "19")]
        Union(super::UnionExecNode),
        #[prost(message, tag = "20")]
        Explain(super::ExplainExecNode),
        #[prost(message, tag = "21")]
        SortPreservingMerge(
            ::prost::alloc::boxed::Box<super::SortPreservingMergeExecNode>,
        ),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalExtensionNode {
    #[prost(bytes = "vec", tag = "1")]
    pub node: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub inputs: ::prost::alloc::vec::Vec<PhysicalPlanNode>,
}
/// physical expressions
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalExprNode {
    #[prost(
        oneof = "physical_expr_node::ExprType",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19"
    )]
    pub expr_type: ::core::option::Option<physical_expr_node::ExprType>,
}
/// Nested message and enum types in `PhysicalExprNode`.
pub mod physical_expr_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum ExprType {
        /// column references
        #[prost(message, tag = "1")]
        Column(super::PhysicalColumn),
        #[prost(message, tag = "2")]
        Literal(super::ScalarValue),
        /// binary expressions
        #[prost(message, tag = "3")]
        BinaryExpr(::prost::alloc::boxed::Box<super::PhysicalBinaryExprNode>),
        /// aggregate expressions
        #[prost(message, tag = "4")]
        AggregateExpr(super::PhysicalAggregateExprNode),
        /// null checks
        #[prost(message, tag = "5")]
        IsNullExpr(::prost::alloc::boxed::Box<super::PhysicalIsNull>),
        #[prost(message, tag = "6")]
        IsNotNullExpr(::prost::alloc::boxed::Box<super::PhysicalIsNotNull>),
        #[prost(message, tag = "7")]
        NotExpr(::prost::alloc::boxed::Box<super::PhysicalNot>),
        #[prost(message, tag = "8")]
        Case(::prost::alloc::boxed::Box<super::PhysicalCaseNode>),
        #[prost(message, tag = "9")]
        Cast(::prost::alloc::boxed::Box<super::PhysicalCastNode>),
        #[prost(message, tag = "10")]
        Sort(::prost::alloc::boxed::Box<super::PhysicalSortExprNode>),
        #[prost(message, tag = "11")]
        Negative(::prost::alloc::boxed::Box<super::PhysicalNegativeNode>),
        #[prost(message, tag = "12")]
        InList(::prost::alloc::boxed::Box<super::PhysicalInListNode>),
        #[prost(message, tag = "13")]
        ScalarFunction(super::PhysicalScalarFunctionNode),
        #[prost(message, tag = "14")]
        TryCast(::prost::alloc::boxed::Box<super::PhysicalTryCastNode>),
        /// window expressions
        #[prost(message, tag = "15")]
        WindowExpr(::prost::alloc::boxed::Box<super::PhysicalWindowExprNode>),
        #[prost(message, tag = "16")]
        ScalarUdf(super::PhysicalScalarUdfNode),
        #[prost(message, tag = "17")]
        DateTimeIntervalExpr(
            ::prost::alloc::boxed::Box<super::PhysicalDateTimeIntervalExprNode>,
        ),
        #[prost(message, tag = "18")]
        LikeExpr(::prost::alloc::boxed::Box<super::PhysicalLikeExprNode>),
        #[prost(message, tag = "19")]
        GetIndexedFieldExpr(
            ::prost::alloc::boxed::Box<super::PhysicalGetIndexedFieldExprNode>,
        ),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalScalarUdfNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub args: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, optional, tag = "4")]
    pub return_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalAggregateExprNode {
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, tag = "3")]
    pub distinct: bool,
    #[prost(oneof = "physical_aggregate_expr_node::AggregateFunction", tags = "1, 4")]
    pub aggregate_function: ::core::option::Option<
        physical_aggregate_expr_node::AggregateFunction,
    >,
}
/// Nested message and enum types in `PhysicalAggregateExprNode`.
pub mod physical_aggregate_expr_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum AggregateFunction {
        #[prost(enumeration = "super::AggregateFunction", tag = "1")]
        AggrFunction(i32),
        #[prost(string, tag = "4")]
        UserDefinedAggrFunction(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalWindowExprNode {
    #[prost(message, optional, boxed, tag = "4")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(oneof = "physical_window_expr_node::WindowFunction", tags = "1, 2")]
    pub window_function: ::core::option::Option<
        physical_window_expr_node::WindowFunction,
    >,
}
/// Nested message and enum types in `PhysicalWindowExprNode`.
pub mod physical_window_expr_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum WindowFunction {
        #[prost(enumeration = "super::AggregateFunction", tag = "1")]
        AggrFunction(i32),
        /// udaf = 3
        #[prost(enumeration = "super::BuiltInWindowFunction", tag = "2")]
        BuiltInFunction(i32),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalIsNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalIsNotNull {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalNot {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalAliasNode {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<PhysicalExprNode>,
    #[prost(string, tag = "2")]
    pub alias: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalBinaryExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub l: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub r: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalDateTimeIntervalExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub l: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub r: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(string, tag = "3")]
    pub op: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalLikeExprNode {
    #[prost(bool, tag = "1")]
    pub negated: bool,
    #[prost(bool, tag = "2")]
    pub case_insensitive: bool,
    #[prost(message, optional, boxed, tag = "3")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, boxed, tag = "4")]
    pub pattern: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalSortExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(bool, tag = "2")]
    pub asc: bool,
    #[prost(bool, tag = "3")]
    pub nulls_first: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalWhenThen {
    #[prost(message, optional, tag = "1")]
    pub when_expr: ::core::option::Option<PhysicalExprNode>,
    #[prost(message, optional, tag = "2")]
    pub then_expr: ::core::option::Option<PhysicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalInListNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub list: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, tag = "3")]
    pub negated: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalCaseNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, repeated, tag = "2")]
    pub when_then_expr: ::prost::alloc::vec::Vec<PhysicalWhenThen>,
    #[prost(message, optional, boxed, tag = "3")]
    pub else_expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalScalarFunctionNode {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration = "ScalarFunction", tag = "2")]
    pub fun: i32,
    #[prost(message, repeated, tag = "3")]
    pub args: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, optional, tag = "4")]
    pub return_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalTryCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalCastNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub arrow_type: ::core::option::Option<ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalNegativeNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub expr: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FilterExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, tag = "2")]
    pub expr: ::core::option::Option<PhysicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileGroup {
    #[prost(message, repeated, tag = "1")]
    pub files: ::prost::alloc::vec::Vec<PartitionedFile>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScanLimit {
    /// wrap into a message to make it optional
    #[prost(uint32, tag = "1")]
    pub limit: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalSortExprNodeCollection {
    #[prost(message, repeated, tag = "1")]
    pub physical_sort_expr_nodes: ::prost::alloc::vec::Vec<PhysicalSortExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileScanExecConf {
    #[prost(message, repeated, tag = "1")]
    pub file_groups: ::prost::alloc::vec::Vec<FileGroup>,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(uint32, repeated, tag = "4")]
    pub projection: ::prost::alloc::vec::Vec<u32>,
    #[prost(message, optional, tag = "5")]
    pub limit: ::core::option::Option<ScanLimit>,
    #[prost(message, optional, tag = "6")]
    pub statistics: ::core::option::Option<Statistics>,
    #[prost(string, repeated, tag = "7")]
    pub table_partition_cols: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "8")]
    pub object_store_url: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "9")]
    pub output_ordering: ::prost::alloc::vec::Vec<PhysicalSortExprNodeCollection>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
    #[prost(message, optional, tag = "3")]
    pub predicate: ::core::option::Option<PhysicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CsvScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
    #[prost(bool, tag = "2")]
    pub has_header: bool,
    #[prost(string, tag = "3")]
    pub delimiter: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AvroScanExecNode {
    #[prost(message, optional, tag = "1")]
    pub base_conf: ::core::option::Option<FileScanExecConf>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HashJoinExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "3")]
    pub on: ::prost::alloc::vec::Vec<JoinOn>,
    #[prost(enumeration = "JoinType", tag = "4")]
    pub join_type: i32,
    #[prost(enumeration = "PartitionMode", tag = "6")]
    pub partition_mode: i32,
    #[prost(bool, tag = "7")]
    pub null_equals_null: bool,
    #[prost(message, optional, tag = "8")]
    pub filter: ::core::option::Option<JoinFilter>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnionExecNode {
    #[prost(message, repeated, tag = "1")]
    pub inputs: ::prost::alloc::vec::Vec<PhysicalPlanNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExplainExecNode {
    #[prost(message, optional, tag = "1")]
    pub schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "2")]
    pub stringified_plans: ::prost::alloc::vec::Vec<StringifiedPlan>,
    #[prost(bool, tag = "3")]
    pub verbose: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CrossJoinExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, optional, boxed, tag = "2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalColumn {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub index: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinOn {
    #[prost(message, optional, tag = "1")]
    pub left: ::core::option::Option<PhysicalColumn>,
    #[prost(message, optional, tag = "2")]
    pub right: ::core::option::Option<PhysicalColumn>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EmptyExecNode {
    #[prost(bool, tag = "1")]
    pub produce_one_row: bool,
    #[prost(message, optional, tag = "2")]
    pub schema: ::core::option::Option<Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProjectionExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(string, repeated, tag = "3")]
    pub expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WindowAggExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub window_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(string, repeated, tag = "3")]
    pub window_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub input_schema: ::core::option::Option<Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaybeFilter {
    #[prost(message, optional, tag = "1")]
    pub expr: ::core::option::Option<PhysicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MaybePhysicalSortExprs {
    #[prost(message, repeated, tag = "1")]
    pub sort_expr: ::prost::alloc::vec::Vec<PhysicalSortExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AggregateExecNode {
    #[prost(message, repeated, tag = "1")]
    pub group_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(message, repeated, tag = "2")]
    pub aggr_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(enumeration = "AggregateMode", tag = "3")]
    pub mode: i32,
    #[prost(message, optional, boxed, tag = "4")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(string, repeated, tag = "5")]
    pub group_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, repeated, tag = "6")]
    pub aggr_expr_name: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// we need the input schema to the partial aggregate to pass to the final aggregate
    #[prost(message, optional, tag = "7")]
    pub input_schema: ::core::option::Option<Schema>,
    #[prost(message, repeated, tag = "8")]
    pub null_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(bool, repeated, tag = "9")]
    pub groups: ::prost::alloc::vec::Vec<bool>,
    #[prost(message, repeated, tag = "10")]
    pub filter_expr: ::prost::alloc::vec::Vec<MaybeFilter>,
    #[prost(message, repeated, tag = "11")]
    pub order_by_expr: ::prost::alloc::vec::Vec<MaybePhysicalSortExprs>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GlobalLimitExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    /// The number of rows to skip before fetch
    #[prost(uint32, tag = "2")]
    pub skip: u32,
    /// Maximum number of rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalLimitExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(uint32, tag = "2")]
    pub fetch: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    /// Maximum number of highest/lowest rows to fetch; negative means no limit
    #[prost(int64, tag = "3")]
    pub fetch: i64,
    #[prost(bool, tag = "4")]
    pub preserve_partitioning: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SortPreservingMergeExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(message, repeated, tag = "2")]
    pub expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoalesceBatchesExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(uint32, tag = "2")]
    pub target_batch_size: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CoalescePartitionsExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalHashRepartition {
    #[prost(message, repeated, tag = "1")]
    pub hash_expr: ::prost::alloc::vec::Vec<PhysicalExprNode>,
    #[prost(uint64, tag = "2")]
    pub partition_count: u64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RepartitionExecNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub input: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalPlanNode>>,
    #[prost(oneof = "repartition_exec_node::PartitionMethod", tags = "2, 3, 4")]
    pub partition_method: ::core::option::Option<repartition_exec_node::PartitionMethod>,
}
/// Nested message and enum types in `RepartitionExecNode`.
pub mod repartition_exec_node {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum PartitionMethod {
        #[prost(uint64, tag = "2")]
        RoundRobin(u64),
        #[prost(message, tag = "3")]
        Hash(super::PhysicalHashRepartition),
        #[prost(uint64, tag = "4")]
        Unknown(u64),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinFilter {
    #[prost(message, optional, tag = "1")]
    pub expression: ::core::option::Option<PhysicalExprNode>,
    #[prost(message, repeated, tag = "2")]
    pub column_indices: ::prost::alloc::vec::Vec<ColumnIndex>,
    #[prost(message, optional, tag = "3")]
    pub schema: ::core::option::Option<Schema>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnIndex {
    #[prost(uint32, tag = "1")]
    pub index: u32,
    #[prost(enumeration = "JoinSide", tag = "2")]
    pub side: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionedFile {
    #[prost(string, tag = "1")]
    pub path: ::prost::alloc::string::String,
    #[prost(uint64, tag = "2")]
    pub size: u64,
    #[prost(uint64, tag = "3")]
    pub last_modified_ns: u64,
    #[prost(message, repeated, tag = "4")]
    pub partition_values: ::prost::alloc::vec::Vec<ScalarValue>,
    #[prost(message, optional, tag = "5")]
    pub range: ::core::option::Option<FileRange>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileRange {
    #[prost(int64, tag = "1")]
    pub start: i64,
    #[prost(int64, tag = "2")]
    pub end: i64,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartitionStats {
    #[prost(int64, tag = "1")]
    pub num_rows: i64,
    #[prost(int64, tag = "2")]
    pub num_batches: i64,
    #[prost(int64, tag = "3")]
    pub num_bytes: i64,
    #[prost(message, repeated, tag = "4")]
    pub column_stats: ::prost::alloc::vec::Vec<ColumnStats>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Statistics {
    #[prost(int64, tag = "1")]
    pub num_rows: i64,
    #[prost(int64, tag = "2")]
    pub total_byte_size: i64,
    #[prost(message, repeated, tag = "3")]
    pub column_stats: ::prost::alloc::vec::Vec<ColumnStats>,
    #[prost(bool, tag = "4")]
    pub is_exact: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnStats {
    #[prost(message, optional, tag = "1")]
    pub min_value: ::core::option::Option<ScalarValue>,
    #[prost(message, optional, tag = "2")]
    pub max_value: ::core::option::Option<ScalarValue>,
    #[prost(uint32, tag = "3")]
    pub null_count: u32,
    #[prost(uint32, tag = "4")]
    pub distinct_count: u32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PhysicalGetIndexedFieldExprNode {
    #[prost(message, optional, boxed, tag = "1")]
    pub arg: ::core::option::Option<::prost::alloc::boxed::Box<PhysicalExprNode>>,
    #[prost(message, optional, tag = "2")]
    pub key: ::core::option::Option<ScalarValue>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinType {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3,
    Leftsemi = 4,
    Leftanti = 5,
    Rightsemi = 6,
    Rightanti = 7,
}
impl JoinType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
            JoinType::Leftsemi => "LEFTSEMI",
            JoinType::Leftanti => "LEFTANTI",
            JoinType::Rightsemi => "RIGHTSEMI",
            JoinType::Rightanti => "RIGHTANTI",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "INNER" => Some(Self::Inner),
            "LEFT" => Some(Self::Left),
            "RIGHT" => Some(Self::Right),
            "FULL" => Some(Self::Full),
            "LEFTSEMI" => Some(Self::Leftsemi),
            "LEFTANTI" => Some(Self::Leftanti),
            "RIGHTSEMI" => Some(Self::Rightsemi),
            "RIGHTANTI" => Some(Self::Rightanti),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinConstraint {
    On = 0,
    Using = 1,
}
impl JoinConstraint {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinConstraint::On => "ON",
            JoinConstraint::Using => "USING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ON" => Some(Self::On),
            "USING" => Some(Self::Using),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ScalarFunction {
    Abs = 0,
    Acos = 1,
    Asin = 2,
    Atan = 3,
    Ascii = 4,
    Ceil = 5,
    Cos = 6,
    Digest = 7,
    Exp = 8,
    Floor = 9,
    Ln = 10,
    Log = 11,
    Log10 = 12,
    Log2 = 13,
    Round = 14,
    Signum = 15,
    Sin = 16,
    Sqrt = 17,
    Tan = 18,
    Trunc = 19,
    Array = 20,
    RegexpMatch = 21,
    BitLength = 22,
    Btrim = 23,
    CharacterLength = 24,
    Chr = 25,
    Concat = 26,
    ConcatWithSeparator = 27,
    DatePart = 28,
    DateTrunc = 29,
    InitCap = 30,
    Left = 31,
    Lpad = 32,
    Lower = 33,
    Ltrim = 34,
    Md5 = 35,
    NullIf = 36,
    OctetLength = 37,
    Random = 38,
    RegexpReplace = 39,
    Repeat = 40,
    Replace = 41,
    Reverse = 42,
    Right = 43,
    Rpad = 44,
    Rtrim = 45,
    Sha224 = 46,
    Sha256 = 47,
    Sha384 = 48,
    Sha512 = 49,
    SplitPart = 50,
    StartsWith = 51,
    Strpos = 52,
    Substr = 53,
    ToHex = 54,
    ToTimestamp = 55,
    ToTimestampMillis = 56,
    ToTimestampMicros = 57,
    ToTimestampSeconds = 58,
    Now = 59,
    Translate = 60,
    Trim = 61,
    Upper = 62,
    Coalesce = 63,
    Power = 64,
    StructFun = 65,
    FromUnixtime = 66,
    Atan2 = 67,
    DateBin = 68,
    ArrowTypeof = 69,
    CurrentDate = 70,
    CurrentTime = 71,
    Uuid = 72,
    Cbrt = 73,
    Acosh = 74,
    Asinh = 75,
    Atanh = 76,
    Sinh = 77,
    Cosh = 78,
    Tanh = 79,
    Pi = 80,
    Degrees = 81,
    Radians = 82,
    Factorial = 83,
    Lcm = 84,
    Gcd = 85,
    Greatest = 86,
    Least = 87,
}
impl ScalarFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ScalarFunction::Abs => "Abs",
            ScalarFunction::Acos => "Acos",
            ScalarFunction::Asin => "Asin",
            ScalarFunction::Atan => "Atan",
            ScalarFunction::Ascii => "Ascii",
            ScalarFunction::Ceil => "Ceil",
            ScalarFunction::Cos => "Cos",
            ScalarFunction::Digest => "Digest",
            ScalarFunction::Exp => "Exp",
            ScalarFunction::Floor => "Floor",
            ScalarFunction::Ln => "Ln",
            ScalarFunction::Log => "Log",
            ScalarFunction::Log10 => "Log10",
            ScalarFunction::Log2 => "Log2",
            ScalarFunction::Round => "Round",
            ScalarFunction::Signum => "Signum",
            ScalarFunction::Sin => "Sin",
            ScalarFunction::Sqrt => "Sqrt",
            ScalarFunction::Tan => "Tan",
            ScalarFunction::Trunc => "Trunc",
            ScalarFunction::Array => "Array",
            ScalarFunction::RegexpMatch => "RegexpMatch",
            ScalarFunction::BitLength => "BitLength",
            ScalarFunction::Btrim => "Btrim",
            ScalarFunction::CharacterLength => "CharacterLength",
            ScalarFunction::Chr => "Chr",
            ScalarFunction::Concat => "Concat",
            ScalarFunction::ConcatWithSeparator => "ConcatWithSeparator",
            ScalarFunction::DatePart => "DatePart",
            ScalarFunction::DateTrunc => "DateTrunc",
            ScalarFunction::InitCap => "InitCap",
            ScalarFunction::Left => "Left",
            ScalarFunction::Lpad => "Lpad",
            ScalarFunction::Lower => "Lower",
            ScalarFunction::Ltrim => "Ltrim",
            ScalarFunction::Md5 => "MD5",
            ScalarFunction::NullIf => "NullIf",
            ScalarFunction::OctetLength => "OctetLength",
            ScalarFunction::Random => "Random",
            ScalarFunction::RegexpReplace => "RegexpReplace",
            ScalarFunction::Repeat => "Repeat",
            ScalarFunction::Replace => "Replace",
            ScalarFunction::Reverse => "Reverse",
            ScalarFunction::Right => "Right",
            ScalarFunction::Rpad => "Rpad",
            ScalarFunction::Rtrim => "Rtrim",
            ScalarFunction::Sha224 => "SHA224",
            ScalarFunction::Sha256 => "SHA256",
            ScalarFunction::Sha384 => "SHA384",
            ScalarFunction::Sha512 => "SHA512",
            ScalarFunction::SplitPart => "SplitPart",
            ScalarFunction::StartsWith => "StartsWith",
            ScalarFunction::Strpos => "Strpos",
            ScalarFunction::Substr => "Substr",
            ScalarFunction::ToHex => "ToHex",
            ScalarFunction::ToTimestamp => "ToTimestamp",
            ScalarFunction::ToTimestampMillis => "ToTimestampMillis",
            ScalarFunction::ToTimestampMicros => "ToTimestampMicros",
            ScalarFunction::ToTimestampSeconds => "ToTimestampSeconds",
            ScalarFunction::Now => "Now",
            ScalarFunction::Translate => "Translate",
            ScalarFunction::Trim => "Trim",
            ScalarFunction::Upper => "Upper",
            ScalarFunction::Coalesce => "Coalesce",
            ScalarFunction::Power => "Power",
            ScalarFunction::StructFun => "StructFun",
            ScalarFunction::FromUnixtime => "FromUnixtime",
            ScalarFunction::Atan2 => "Atan2",
            ScalarFunction::DateBin => "DateBin",
            ScalarFunction::ArrowTypeof => "ArrowTypeof",
            ScalarFunction::CurrentDate => "CurrentDate",
            ScalarFunction::CurrentTime => "CurrentTime",
            ScalarFunction::Uuid => "Uuid",
            ScalarFunction::Cbrt => "Cbrt",
            ScalarFunction::Acosh => "Acosh",
            ScalarFunction::Asinh => "Asinh",
            ScalarFunction::Atanh => "Atanh",
            ScalarFunction::Sinh => "Sinh",
            ScalarFunction::Cosh => "Cosh",
            ScalarFunction::Tanh => "Tanh",
            ScalarFunction::Pi => "Pi",
            ScalarFunction::Degrees => "Degrees",
            ScalarFunction::Radians => "Radians",
            ScalarFunction::Factorial => "Factorial",
            ScalarFunction::Lcm => "Lcm",
            ScalarFunction::Gcd => "Gcd",
            ScalarFunction::Greatest => "Greatest",
            ScalarFunction::Least => "Least",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Abs" => Some(Self::Abs),
            "Acos" => Some(Self::Acos),
            "Asin" => Some(Self::Asin),
            "Atan" => Some(Self::Atan),
            "Ascii" => Some(Self::Ascii),
            "Ceil" => Some(Self::Ceil),
            "Cos" => Some(Self::Cos),
            "Digest" => Some(Self::Digest),
            "Exp" => Some(Self::Exp),
            "Floor" => Some(Self::Floor),
            "Ln" => Some(Self::Ln),
            "Log" => Some(Self::Log),
            "Log10" => Some(Self::Log10),
            "Log2" => Some(Self::Log2),
            "Round" => Some(Self::Round),
            "Signum" => Some(Self::Signum),
            "Sin" => Some(Self::Sin),
            "Sqrt" => Some(Self::Sqrt),
            "Tan" => Some(Self::Tan),
            "Trunc" => Some(Self::Trunc),
            "Array" => Some(Self::Array),
            "RegexpMatch" => Some(Self::RegexpMatch),
            "BitLength" => Some(Self::BitLength),
            "Btrim" => Some(Self::Btrim),
            "CharacterLength" => Some(Self::CharacterLength),
            "Chr" => Some(Self::Chr),
            "Concat" => Some(Self::Concat),
            "ConcatWithSeparator" => Some(Self::ConcatWithSeparator),
            "DatePart" => Some(Self::DatePart),
            "DateTrunc" => Some(Self::DateTrunc),
            "InitCap" => Some(Self::InitCap),
            "Left" => Some(Self::Left),
            "Lpad" => Some(Self::Lpad),
            "Lower" => Some(Self::Lower),
            "Ltrim" => Some(Self::Ltrim),
            "MD5" => Some(Self::Md5),
            "NullIf" => Some(Self::NullIf),
            "OctetLength" => Some(Self::OctetLength),
            "Random" => Some(Self::Random),
            "RegexpReplace" => Some(Self::RegexpReplace),
            "Repeat" => Some(Self::Repeat),
            "Replace" => Some(Self::Replace),
            "Reverse" => Some(Self::Reverse),
            "Right" => Some(Self::Right),
            "Rpad" => Some(Self::Rpad),
            "Rtrim" => Some(Self::Rtrim),
            "SHA224" => Some(Self::Sha224),
            "SHA256" => Some(Self::Sha256),
            "SHA384" => Some(Self::Sha384),
            "SHA512" => Some(Self::Sha512),
            "SplitPart" => Some(Self::SplitPart),
            "StartsWith" => Some(Self::StartsWith),
            "Strpos" => Some(Self::Strpos),
            "Substr" => Some(Self::Substr),
            "ToHex" => Some(Self::ToHex),
            "ToTimestamp" => Some(Self::ToTimestamp),
            "ToTimestampMillis" => Some(Self::ToTimestampMillis),
            "ToTimestampMicros" => Some(Self::ToTimestampMicros),
            "ToTimestampSeconds" => Some(Self::ToTimestampSeconds),
            "Now" => Some(Self::Now),
            "Translate" => Some(Self::Translate),
            "Trim" => Some(Self::Trim),
            "Upper" => Some(Self::Upper),
            "Coalesce" => Some(Self::Coalesce),
            "Power" => Some(Self::Power),
            "StructFun" => Some(Self::StructFun),
            "FromUnixtime" => Some(Self::FromUnixtime),
            "Atan2" => Some(Self::Atan2),
            "DateBin" => Some(Self::DateBin),
            "ArrowTypeof" => Some(Self::ArrowTypeof),
            "CurrentDate" => Some(Self::CurrentDate),
            "CurrentTime" => Some(Self::CurrentTime),
            "Uuid" => Some(Self::Uuid),
            "Cbrt" => Some(Self::Cbrt),
            "Acosh" => Some(Self::Acosh),
            "Asinh" => Some(Self::Asinh),
            "Atanh" => Some(Self::Atanh),
            "Sinh" => Some(Self::Sinh),
            "Cosh" => Some(Self::Cosh),
            "Tanh" => Some(Self::Tanh),
            "Pi" => Some(Self::Pi),
            "Degrees" => Some(Self::Degrees),
            "Radians" => Some(Self::Radians),
            "Factorial" => Some(Self::Factorial),
            "Lcm" => Some(Self::Lcm),
            "Gcd" => Some(Self::Gcd),
            "Greatest" => Some(Self::Greatest),
            "Least" => Some(Self::Least),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateFunction {
    Min = 0,
    Max = 1,
    Sum = 2,
    Avg = 3,
    Count = 4,
    ApproxDistinct = 5,
    ArrayAgg = 6,
    Variance = 7,
    VariancePop = 8,
    Covariance = 9,
    CovariancePop = 10,
    Stddev = 11,
    StddevPop = 12,
    Correlation = 13,
    ApproxPercentileCont = 14,
    ApproxMedian = 15,
    ApproxPercentileContWithWeight = 16,
    Grouping = 17,
    Median = 18,
    BitAnd = 19,
    BitOr = 20,
    BitXor = 21,
    BoolAnd = 22,
    BoolOr = 23,
    /// When a function with the same name exists among built-in window functions,
    /// we append "_AGG" to obey name scoping rules.
    FirstValueAgg = 24,
    LastValueAgg = 25,
}
impl AggregateFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregateFunction::Min => "MIN",
            AggregateFunction::Max => "MAX",
            AggregateFunction::Sum => "SUM",
            AggregateFunction::Avg => "AVG",
            AggregateFunction::Count => "COUNT",
            AggregateFunction::ApproxDistinct => "APPROX_DISTINCT",
            AggregateFunction::ArrayAgg => "ARRAY_AGG",
            AggregateFunction::Variance => "VARIANCE",
            AggregateFunction::VariancePop => "VARIANCE_POP",
            AggregateFunction::Covariance => "COVARIANCE",
            AggregateFunction::CovariancePop => "COVARIANCE_POP",
            AggregateFunction::Stddev => "STDDEV",
            AggregateFunction::StddevPop => "STDDEV_POP",
            AggregateFunction::Correlation => "CORRELATION",
            AggregateFunction::ApproxPercentileCont => "APPROX_PERCENTILE_CONT",
            AggregateFunction::ApproxMedian => "APPROX_MEDIAN",
            AggregateFunction::ApproxPercentileContWithWeight => {
                "APPROX_PERCENTILE_CONT_WITH_WEIGHT"
            }
            AggregateFunction::Grouping => "GROUPING",
            AggregateFunction::Median => "MEDIAN",
            AggregateFunction::BitAnd => "BIT_AND",
            AggregateFunction::BitOr => "BIT_OR",
            AggregateFunction::BitXor => "BIT_XOR",
            AggregateFunction::BoolAnd => "BOOL_AND",
            AggregateFunction::BoolOr => "BOOL_OR",
            AggregateFunction::FirstValueAgg => "FIRST_VALUE_AGG",
            AggregateFunction::LastValueAgg => "LAST_VALUE_AGG",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "SUM" => Some(Self::Sum),
            "AVG" => Some(Self::Avg),
            "COUNT" => Some(Self::Count),
            "APPROX_DISTINCT" => Some(Self::ApproxDistinct),
            "ARRAY_AGG" => Some(Self::ArrayAgg),
            "VARIANCE" => Some(Self::Variance),
            "VARIANCE_POP" => Some(Self::VariancePop),
            "COVARIANCE" => Some(Self::Covariance),
            "COVARIANCE_POP" => Some(Self::CovariancePop),
            "STDDEV" => Some(Self::Stddev),
            "STDDEV_POP" => Some(Self::StddevPop),
            "CORRELATION" => Some(Self::Correlation),
            "APPROX_PERCENTILE_CONT" => Some(Self::ApproxPercentileCont),
            "APPROX_MEDIAN" => Some(Self::ApproxMedian),
            "APPROX_PERCENTILE_CONT_WITH_WEIGHT" => {
                Some(Self::ApproxPercentileContWithWeight)
            }
            "GROUPING" => Some(Self::Grouping),
            "MEDIAN" => Some(Self::Median),
            "BIT_AND" => Some(Self::BitAnd),
            "BIT_OR" => Some(Self::BitOr),
            "BIT_XOR" => Some(Self::BitXor),
            "BOOL_AND" => Some(Self::BoolAnd),
            "BOOL_OR" => Some(Self::BoolOr),
            "FIRST_VALUE_AGG" => Some(Self::FirstValueAgg),
            "LAST_VALUE_AGG" => Some(Self::LastValueAgg),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BuiltInWindowFunction {
    RowNumber = 0,
    Rank = 1,
    DenseRank = 2,
    PercentRank = 3,
    CumeDist = 4,
    Ntile = 5,
    Lag = 6,
    Lead = 7,
    FirstValue = 8,
    LastValue = 9,
    NthValue = 10,
}
impl BuiltInWindowFunction {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BuiltInWindowFunction::RowNumber => "ROW_NUMBER",
            BuiltInWindowFunction::Rank => "RANK",
            BuiltInWindowFunction::DenseRank => "DENSE_RANK",
            BuiltInWindowFunction::PercentRank => "PERCENT_RANK",
            BuiltInWindowFunction::CumeDist => "CUME_DIST",
            BuiltInWindowFunction::Ntile => "NTILE",
            BuiltInWindowFunction::Lag => "LAG",
            BuiltInWindowFunction::Lead => "LEAD",
            BuiltInWindowFunction::FirstValue => "FIRST_VALUE",
            BuiltInWindowFunction::LastValue => "LAST_VALUE",
            BuiltInWindowFunction::NthValue => "NTH_VALUE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ROW_NUMBER" => Some(Self::RowNumber),
            "RANK" => Some(Self::Rank),
            "DENSE_RANK" => Some(Self::DenseRank),
            "PERCENT_RANK" => Some(Self::PercentRank),
            "CUME_DIST" => Some(Self::CumeDist),
            "NTILE" => Some(Self::Ntile),
            "LAG" => Some(Self::Lag),
            "LEAD" => Some(Self::Lead),
            "FIRST_VALUE" => Some(Self::FirstValue),
            "LAST_VALUE" => Some(Self::LastValue),
            "NTH_VALUE" => Some(Self::NthValue),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WindowFrameUnits {
    Rows = 0,
    Range = 1,
    Groups = 2,
}
impl WindowFrameUnits {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WindowFrameUnits::Rows => "ROWS",
            WindowFrameUnits::Range => "RANGE",
            WindowFrameUnits::Groups => "GROUPS",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ROWS" => Some(Self::Rows),
            "RANGE" => Some(Self::Range),
            "GROUPS" => Some(Self::Groups),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum WindowFrameBoundType {
    CurrentRow = 0,
    Preceding = 1,
    Following = 2,
}
impl WindowFrameBoundType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            WindowFrameBoundType::CurrentRow => "CURRENT_ROW",
            WindowFrameBoundType::Preceding => "PRECEDING",
            WindowFrameBoundType::Following => "FOLLOWING",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CURRENT_ROW" => Some(Self::CurrentRow),
            "PRECEDING" => Some(Self::Preceding),
            "FOLLOWING" => Some(Self::Following),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum DateUnit {
    Day = 0,
    DateMillisecond = 1,
}
impl DateUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            DateUnit::Day => "Day",
            DateUnit::DateMillisecond => "DateMillisecond",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Day" => Some(Self::Day),
            "DateMillisecond" => Some(Self::DateMillisecond),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Second = 0,
    Millisecond = 1,
    Microsecond = 2,
    Nanosecond = 3,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Second => "Second",
            TimeUnit::Millisecond => "Millisecond",
            TimeUnit::Microsecond => "Microsecond",
            TimeUnit::Nanosecond => "Nanosecond",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "Second" => Some(Self::Second),
            "Millisecond" => Some(Self::Millisecond),
            "Microsecond" => Some(Self::Microsecond),
            "Nanosecond" => Some(Self::Nanosecond),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum IntervalUnit {
    YearMonth = 0,
    DayTime = 1,
    MonthDayNano = 2,
}
impl IntervalUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            IntervalUnit::YearMonth => "YearMonth",
            IntervalUnit::DayTime => "DayTime",
            IntervalUnit::MonthDayNano => "MonthDayNano",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "YearMonth" => Some(Self::YearMonth),
            "DayTime" => Some(Self::DayTime),
            "MonthDayNano" => Some(Self::MonthDayNano),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UnionMode {
    Sparse = 0,
    Dense = 1,
}
impl UnionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UnionMode::Sparse => "sparse",
            UnionMode::Dense => "dense",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "sparse" => Some(Self::Sparse),
            "dense" => Some(Self::Dense),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum PartitionMode {
    CollectLeft = 0,
    Partitioned = 1,
    Auto = 2,
}
impl PartitionMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            PartitionMode::CollectLeft => "COLLECT_LEFT",
            PartitionMode::Partitioned => "PARTITIONED",
            PartitionMode::Auto => "AUTO",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "COLLECT_LEFT" => Some(Self::CollectLeft),
            "PARTITIONED" => Some(Self::Partitioned),
            "AUTO" => Some(Self::Auto),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AggregateMode {
    Partial = 0,
    Final = 1,
    FinalPartitioned = 2,
    Single = 3,
}
impl AggregateMode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            AggregateMode::Partial => "PARTIAL",
            AggregateMode::Final => "FINAL",
            AggregateMode::FinalPartitioned => "FINAL_PARTITIONED",
            AggregateMode::Single => "SINGLE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PARTIAL" => Some(Self::Partial),
            "FINAL" => Some(Self::Final),
            "FINAL_PARTITIONED" => Some(Self::FinalPartitioned),
            "SINGLE" => Some(Self::Single),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum JoinSide {
    LeftSide = 0,
    RightSide = 1,
}
impl JoinSide {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            JoinSide::LeftSide => "LEFT_SIDE",
            JoinSide::RightSide => "RIGHT_SIDE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LEFT_SIDE" => Some(Self::LeftSide),
            "RIGHT_SIDE" => Some(Self::RightSide),
            _ => None,
        }
    }
}
