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

//! This module contains utilities to manipulate avro metadata.

#[cfg(feature = "avro")]
mod arrow_array_reader;
#[cfg(feature = "avro")]
mod reader;
#[cfg(feature = "avro")]
mod schema;

use crate::arrow::datatypes::Schema;
use crate::error::Result;
#[cfg(feature = "avro")]
pub use reader::{Reader, ReaderBuilder};
use std::io::Read;

#[cfg(feature = "avro")]
/// Read Avro schema given a reader
pub fn read_avro_schema_from_reader<R: Read>(reader: &mut R) -> Result<Schema> {
    let avro_reader = apache_avro::Reader::new(reader)?;
    let schema = avro_reader.writer_schema();
    schema::to_arrow_schema(schema)
}

#[cfg(not(feature = "avro"))]
/// Read Avro schema given a reader (requires the avro feature)
pub fn read_avro_schema_from_reader<R: Read>(_: &mut R) -> Result<Schema> {
    Err(crate::error::DataFusionError::NotImplemented(
        "cannot read avro schema without the 'avro' feature enabled".to_string(),
    ))
}
