use std::{borrow::Cow, fmt::Display, sync::Arc};

use crate::error::Result;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};

#[derive(Clone, Debug)]
pub enum LogicalType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    String,
    LargeString,
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Timestamp(TimeUnit, Option<Arc<str>>),
    Duration(TimeUnit),
    Interval(IntervalUnit),
    Binary,
    FixedSizeBinary(i32),
    LargeBinary,
    Utf8,
    LargeUtf8,
    List(Box<LogicalType>),
    FixedSizeList(Box<LogicalType>, i32),
    LargeList(Box<LogicalType>),
    Struct(Fields),
    Map(NamedLogicalTypeRef, bool),
    // union
    Decimal128(u8, i8),
    Decimal256(u8, i8),
    Extension(ExtensionTypeRef),
}

impl PartialEq for LogicalType {
    fn eq(&self, other: &Self) -> bool {
        self.type_signature() == other.type_signature()
    }
}

impl Eq for LogicalType {}

impl std::hash::Hash for LogicalType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.type_signature().hash(state)
    }
}

impl Display for LogicalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

pub type Fields = Arc<[NamedLogicalTypeRef]>;
pub type NamedLogicalTypeRef = Arc<NamedLogicalType>;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct NamedLogicalType {
    name: String,
    data_type: LogicalType,
}

impl NamedLogicalType {
    pub fn new(name: impl Into<String>, data_type: LogicalType) -> Self {
        Self {
            name: name.into(),
            data_type,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &LogicalType {
        &self.data_type
    }
}

pub type OwnedTypeSignature = TypeSignature<'static>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TypeSignature<'a> {
    // **func_name**(p1, p2)
    name: Cow<'a, str>,
    // func_name(**p1**, **p2**)
    params: Vec<Cow<'a, str>>,
}

impl<'a> TypeSignature<'a> {
    pub fn new(name: impl Into<Cow<'a, str>>) -> Self {
        Self::new_with_params(name, vec![])
    }

    pub fn new_with_params(
        name: impl Into<Cow<'a, str>>,
        params: Vec<Cow<'a, str>>,
    ) -> Self {
        Self {
            name: name.into(),
            params,
        }
    }

    pub fn to_owned_type_signature(&self) -> OwnedTypeSignature {
        OwnedTypeSignature {
            name: self.name.to_string().into(),
            params: self.params.iter().map(|p| p.to_string().into()).collect(),
        }
    }
}

pub type ExtensionTypeRef = Arc<dyn ExtensionType + Send + Sync>;

pub trait ExtensionType: std::fmt::Debug {
    fn display_name(&self) -> &str;
    fn type_signature(&self) -> TypeSignature;
    fn physical_type(&self) -> DataType;

    fn is_comparable(&self) -> bool;
    fn is_orderable(&self) -> bool;
    fn is_numeric(&self) -> bool;
}

pub trait TypeManager {
    fn register_data_type(
        &mut self,
        signature: impl Into<TypeSignature<'static>>,
        extension_type: ExtensionTypeRef,
    ) -> Result<()>;

    fn data_type(&self, signature: &TypeSignature) -> Result<Option<ExtensionTypeRef>>;
}

impl ExtensionType for LogicalType {
    fn display_name(&self) -> &str {
        match self {
            Self::Null => "NULL",
            Self::Boolean => "BOOLEAN",
            Self::Int8 => "INT8",
            Self::Int16 => "INT16",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::UInt8 => "UINT8",
            Self::UInt16 => "UINT16",
            Self::UInt32 => "UINT32",
            Self::UInt64 => "UINT64",
            Self::Float16 => "FLOAT16",
            Self::Float32 => "Float16",
            Self::Float64 => "Float64",
            Self::String => "String",
            Self::LargeString => "LargeString",
            Self::Date32 => "Date32",
            Self::Date64 => "Date64",
            Self::Time32(_) => "Time32",
            Self::Time64(_) => "Time64",
            Self::Timestamp(_, _) => "Timestamp",
            Self::Duration(_) => "Duration",
            Self::Interval(_) => "Interval",
            Self::Binary => "Binary",
            Self::FixedSizeBinary(_) => "FixedSizeBinary",
            Self::LargeBinary => "LargeBinary",
            Self::Utf8 => "Utf8",
            Self::LargeUtf8 => "LargeUtf8",
            Self::List(_) => "List",
            Self::FixedSizeList(_, _) => "FixedSizeList",
            Self::LargeList(_) => "LargeList",
            Self::Struct(_) => "Struct",
            Self::Map(_, _) => "Map",
            Self::Decimal128(_, _) => "Decimal128",
            Self::Decimal256(_, _) => "Decimal256",
            Self::Extension(ext) => ext.display_name(),
        }
    }

    fn type_signature(&self) -> TypeSignature {
        match self {
            Self::Boolean => TypeSignature::new("boolean"),
            Self::Int32 => TypeSignature::new("int32"),
            Self::Int64 => TypeSignature::new("int64"),
            Self::UInt64 => TypeSignature::new("uint64"),
            Self::Float32 => TypeSignature::new("float32"),
            Self::Float64 => TypeSignature::new("float64"),
            Self::String => TypeSignature::new("string"),
            Self::Timestamp(tu, zone) => {
                let tu = match tu {
                    TimeUnit::Second => "second",
                    TimeUnit::Millisecond => "millisecond",
                    TimeUnit::Microsecond => "microsecond",
                    TimeUnit::Nanosecond => "nanosecond",
                };

                let params = if let Some(zone) = zone {
                    vec![tu.into(), zone.as_ref().into()]
                } else {
                    vec![tu.into()]
                };

                TypeSignature::new_with_params("timestamp", params)
            }
            Self::Binary => TypeSignature::new("binary"),
            Self::Utf8 => TypeSignature::new("string"),
            Self::Extension(ext) => ext.type_signature(),
            Self::Struct(fields) => {
                let params = fields.iter().map(|f| f.name().into()).collect();
                TypeSignature::new_with_params("struct", params)
            }
            other => panic!("not implemented: {other:?}"),
        }
    }

    fn physical_type(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::String => DataType::Utf8,
            Self::Timestamp(tu, zone) => DataType::Timestamp(tu.clone(), zone.clone()),
            Self::Binary => DataType::Binary,
            Self::Utf8 => DataType::Utf8,
            Self::Extension(ext) => ext.physical_type(),
            Self::Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| {
                        let name = f.name();
                        let data_type = f.physical_type();
                        Arc::new(Field::new(name, data_type, true))
                    })
                    .collect::<Vec<_>>();
                DataType::Struct(fields.into())
            }
            other => panic!("not implemented {other:?}"),
        }
    }

    fn is_comparable(&self) -> bool {
        match self {
            Self::Null
            | Self::Boolean
            | Self::Int8
            | Self::Int16
            | Self::Int32
            | Self::Int64
            | Self::UInt8
            | Self::UInt16
            | Self::UInt32
            | Self::UInt64
            | Self::Float16
            | Self::Float32
            | Self::Float64
            | Self::String
            | Self::LargeString
            | Self::Date32
            | Self::Date64
            | Self::Time32(_)
            | Self::Time64(_)
            | Self::Timestamp(_, _)
            | Self::Duration(_)
            | Self::Interval(_)
            | Self::Binary
            | Self::FixedSizeBinary(_)
            | Self::LargeBinary
            | Self::Utf8
            | Self::LargeUtf8
            | Self::Decimal128(_, _)
            | Self::Decimal256(_, _) => true,
            Self::List(_) => false,
            Self::FixedSizeList(_, _) => false,
            Self::LargeList(_) => false,
            Self::Struct(_) => false,
            Self::Map(_, _) => false,
            Self::Extension(ext) => ext.is_comparable(),
        }
    }

    fn is_orderable(&self) -> bool {
        todo!()
    }

    /// Returns true if this type is numeric: (UInt*, Int*, Float*, Decimal*).
    #[inline]
    fn is_numeric(&self) -> bool {
        use LogicalType::*;
        match self {
            UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _) => true,
            Extension(t) => t.is_numeric(),
            _ => false,
        }
    }
}

impl From<&DataType> for LogicalType {
    fn from(value: &DataType) -> Self {
        // TODO
        value.clone().into()
    }
}

impl From<DataType> for LogicalType {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Null => LogicalType::Null,
            DataType::Boolean => LogicalType::Boolean,
            DataType::Int8 => LogicalType::Int8,
            DataType::Int16 => LogicalType::Int16,
            DataType::Int32 => LogicalType::Int32,
            DataType::Int64 => LogicalType::Int64,
            DataType::UInt8 => LogicalType::UInt8,
            DataType::UInt16 => LogicalType::UInt16,
            DataType::UInt32 => LogicalType::UInt32,
            DataType::UInt64 => LogicalType::UInt64,
            DataType::Float16 => LogicalType::Float16,
            DataType::Float32 => LogicalType::Float32,
            DataType::Float64 => LogicalType::Float64,
            DataType::Timestamp(tu, z) => LogicalType::Timestamp(tu, z),
            DataType::Date32 => LogicalType::Date32,
            DataType::Date64 => LogicalType::Date64,
            DataType::Time32(tu) => LogicalType::Time32(tu),
            DataType::Time64(tu) => LogicalType::Time64(tu),
            DataType::Duration(tu) => LogicalType::Duration(tu),
            DataType::Interval(iu) => LogicalType::Interval(iu),
            DataType::Binary => LogicalType::Binary,
            DataType::FixedSizeBinary(len) => LogicalType::FixedSizeBinary(len),
            DataType::LargeBinary => LogicalType::LargeBinary,
            DataType::Utf8 => LogicalType::Utf8,
            DataType::LargeUtf8 => LogicalType::LargeUtf8,
            DataType::List(f) => LogicalType::List(Box::new(f.data_type().into())),
            DataType::FixedSizeList(f, len) => {
                LogicalType::FixedSizeList(Box::new(f.data_type().into()), len)
            }
            DataType::LargeList(f) => {
                LogicalType::LargeList(Box::new(f.data_type().into()))
            }
            DataType::Struct(fields) => {
                let fields = fields
                    .into_iter()
                    .map(|f| {
                        let name = f.name();
                        let logical_type = f.data_type().into();
                        Arc::new(NamedLogicalType::new(name, logical_type))
                    })
                    .collect::<Vec<_>>();
                LogicalType::Struct(fields.into())
            }
            DataType::Union(_, _) => unimplemented!(),
            DataType::Dictionary(_, dt) => dt.as_ref().into(),
            DataType::Decimal128(p, s) => LogicalType::Decimal128(p, s),
            DataType::Decimal256(p, s) => LogicalType::Decimal256(p, s),
            DataType::Map(data, sorted) => {
                let field =
                    Arc::new(NamedLogicalType::new(data.name(), data.data_type().into()));
                LogicalType::Map(field, sorted)
            }
            DataType::RunEndEncoded(_, f) => f.data_type().into(),
        }
    }
}

impl ExtensionType for NamedLogicalType {
    fn display_name(&self) -> &str {
        &self.name
    }

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::new(self.name())
    }

    fn physical_type(&self) -> DataType {
        self.data_type.physical_type()
    }

    fn is_comparable(&self) -> bool {
        self.data_type.is_comparable()
    }

    fn is_orderable(&self) -> bool {
        self.data_type.is_orderable()
    }

    fn is_numeric(&self) -> bool {
        self.data_type.is_numeric()
    }
}
