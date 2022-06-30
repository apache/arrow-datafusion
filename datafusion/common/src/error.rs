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

//! DataFusion error types

use std::error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use crate::DFSchema;
#[cfg(feature = "avro")]
use apache_avro::Error as AvroError;
use arrow::error::ArrowError;
#[cfg(feature = "jit")]
use cranelift_module::ModuleError;
#[cfg(feature = "parquet")]
use parquet::errors::ParquetError;
use sqlparser::parser::ParserError;

/// Result type for operations that could result in an [DataFusionError]
pub type Result<T> = result::Result<T, DataFusionError>;

/// Error type for generic operations that could result in DataFusionError::External
pub type GenericError = Box<dyn error::Error + Send + Sync>;

/// DataFusion error
#[derive(Debug)]
pub enum DataFusionError {
    /// Error returned by arrow.
    ArrowError(ArrowError),
    /// Wraps an error from the Parquet crate
    #[cfg(feature = "parquet")]
    ParquetError(ParquetError),
    /// Wraps an error from the Avro crate
    #[cfg(feature = "avro")]
    AvroError(AvroError),
    /// Error associated to I/O operations and associated traits.
    IoError(io::Error),
    /// Error returned when SQL is syntactically incorrect.
    SQL(ParserError),
    /// Error returned on a branch that we know it is possible
    /// but to which we still have no implementation for.
    /// Often, these errors are tracked in our issue tracker.
    NotImplemented(String),
    /// Error returned as a consequence of an error in DataFusion.
    /// This error should not happen in normal usage of DataFusion.
    // DataFusions has internal invariants that we are unable to ask the compiler to check for us.
    // This error is raised when one of those invariants is not verified during execution.
    Internal(String),
    /// This error happens whenever a plan is not valid. Examples include
    /// impossible casts.
    Plan(String),
    /// This error happens with schema-related errors, such as schema inference not possible
    /// and non-unique column names.
    SchemaError(SchemaError),
    /// Error returned during execution of the query.
    /// Examples include files not found, errors in parsing certain types.
    Execution(String),
    /// This error is thrown when a consumer cannot acquire memory from the Memory Manager
    /// we can just cancel the execution of the partition.
    ResourcesExhausted(String),
    /// Errors originating from outside DataFusion's core codebase.
    /// For example, a custom S3Error from the crate datafusion-objectstore-s3
    External(GenericError),
    #[cfg(feature = "jit")]
    /// Error occurs during code generation
    JITError(ModuleError),
}

/// Schema-related errors
#[derive(Debug)]
pub enum SchemaError {
    /// Schema contains a (possibly) qualified and unqualified field with same unqualified name
    AmbiguousReference {
        qualifier: Option<String>,
        name: String,
    },
    /// Schema contains duplicate qualified field name
    DuplicateQualifiedField { qualifier: String, name: String },
    /// Schema contains duplicate unqualified field name
    DuplicateUnqualifiedField { name: String },
    /// No field with this name
    FieldNotFound {
        qualifier: Option<String>,
        name: String,
        valid_fields: Option<Vec<String>>,
    },
}

/// Create a "field not found" DataFusion::SchemaError
pub fn field_not_found(
    qualifier: Option<String>,
    name: &str,
    schema: &DFSchema,
) -> DataFusionError {
    DataFusionError::SchemaError(SchemaError::FieldNotFound {
        qualifier,
        name: name.to_string(),
        valid_fields: Some(schema.field_names()),
    })
}

impl Display for SchemaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldNotFound {
                qualifier,
                name,
                valid_fields,
            } => {
                write!(f, "No field named ")?;
                if let Some(q) = qualifier {
                    write!(f, "'{}.{}'", q, name)?;
                } else {
                    write!(f, "'{}'", name)?;
                }
                if let Some(field_names) = valid_fields {
                    write!(
                        f,
                        ". Valid fields are {}",
                        field_names
                            .iter()
                            .map(|name| format!("'{}'", name))
                            .collect::<Vec<String>>()
                            .join(", ")
                    )?;
                }
                write!(f, ".")
            }
            Self::DuplicateQualifiedField { qualifier, name } => {
                write!(
                    f,
                    "Schema contains duplicate qualified field name '{}.{}'",
                    qualifier, name
                )
            }
            Self::DuplicateUnqualifiedField { name } => {
                write!(
                    f,
                    "Schema contains duplicate unqualified field name '{}'",
                    name
                )
            }
            Self::AmbiguousReference { qualifier, name } => {
                if let Some(q) = qualifier {
                    write!(f, "Schema contains qualified field name '{}.{}' and unqualified field name '{}' which would be ambiguous", q, name, name)
                } else {
                    write!(f, "Ambiguous reference to unqualified field '{}'", name)
                }
            }
        }
    }
}

impl From<io::Error> for DataFusionError {
    fn from(e: io::Error) -> Self {
        DataFusionError::IoError(e)
    }
}

impl From<ArrowError> for DataFusionError {
    fn from(e: ArrowError) -> Self {
        DataFusionError::ArrowError(e)
    }
}

impl From<DataFusionError> for ArrowError {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::ArrowError(e) => e,
            DataFusionError::External(e) => ArrowError::ExternalError(e),
            other => ArrowError::ExternalError(Box::new(other)),
        }
    }
}

#[cfg(feature = "parquet")]
impl From<ParquetError> for DataFusionError {
    fn from(e: ParquetError) -> Self {
        DataFusionError::ParquetError(e)
    }
}

#[cfg(feature = "avro")]
impl From<AvroError> for DataFusionError {
    fn from(e: AvroError) -> Self {
        DataFusionError::AvroError(e)
    }
}

impl From<ParserError> for DataFusionError {
    fn from(e: ParserError) -> Self {
        DataFusionError::SQL(e)
    }
}

#[cfg(feature = "jit")]
impl From<ModuleError> for DataFusionError {
    fn from(e: ModuleError) -> Self {
        DataFusionError::JITError(e)
    }
}

impl From<GenericError> for DataFusionError {
    fn from(err: GenericError) -> Self {
        DataFusionError::External(err)
    }
}

impl Display for DataFusionError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            DataFusionError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            #[cfg(feature = "parquet")]
            DataFusionError::ParquetError(ref desc) => {
                write!(f, "Parquet error: {}", desc)
            }
            #[cfg(feature = "avro")]
            DataFusionError::AvroError(ref desc) => {
                write!(f, "Avro error: {}", desc)
            }
            DataFusionError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            DataFusionError::SQL(ref desc) => {
                write!(f, "SQL error: {:?}", desc)
            }
            DataFusionError::NotImplemented(ref desc) => {
                write!(f, "This feature is not implemented: {}", desc)
            }
            DataFusionError::Internal(ref desc) => {
                write!(f, "Internal error: {}. This was likely caused by a bug in DataFusion's \
                    code and we would welcome that you file an bug report in our issue tracker", desc)
            }
            DataFusionError::Plan(ref desc) => {
                write!(f, "Error during planning: {}", desc)
            }
            DataFusionError::SchemaError(ref desc) => {
                write!(f, "Schema error: {}", desc)
            }
            DataFusionError::Execution(ref desc) => {
                write!(f, "Execution error: {}", desc)
            }
            DataFusionError::ResourcesExhausted(ref desc) => {
                write!(f, "Resources exhausted: {}", desc)
            }
            DataFusionError::External(ref desc) => {
                write!(f, "External error: {}", desc)
            }
            #[cfg(feature = "jit")]
            DataFusionError::JITError(ref desc) => {
                write!(f, "JIT error: {}", desc)
            }
        }
    }
}

impl error::Error for DataFusionError {}

#[cfg(test)]
mod test {
    use crate::error::DataFusionError;
    use arrow::error::ArrowError;

    #[test]
    fn arrow_error_to_datafusion() {
        let res = return_arrow_error().unwrap_err();
        assert_eq!(
            res.to_string(),
            "External error: Error during planning: foo"
        );
    }

    #[test]
    fn datafusion_error_to_arrow() {
        let res = return_datafusion_error().unwrap_err();
        assert_eq!(res.to_string(), "Arrow error: Schema error: bar");
    }

    /// Model what happens when implementing SendableRecrordBatchStream:
    /// DataFusion code needs to return an ArrowError
    #[allow(clippy::try_err)]
    fn return_arrow_error() -> arrow::error::Result<()> {
        // Expect the '?' to work
        Err(DataFusionError::Plan("foo".to_string()))?;
        Ok(())
    }

    /// Model what happens when using arrow kernels in DataFusion
    /// code: need to turn an ArrowError into a DataFusionError
    #[allow(clippy::try_err)]
    fn return_datafusion_error() -> crate::error::Result<()> {
        // Expect the '?' to work
        Err(ArrowError::SchemaError("bar".to_string()))?;
        Ok(())
    }
}

#[macro_export]
macro_rules! internal_err {
    ($($arg:tt)*) => {
        Err(DataFusionError::Internal(format!($($arg)*)))
    };
}
