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

use crate::config::ConfigOptions;
use crate::error::_not_impl_err;
use crate::file_options::StatementOptions;
use crate::{DataFusionError, Result};

/// Stub implementation of `ParquetWriterOptions` that always returns a
/// NotYetImplemented error used when parquet feature is not activated.
#[derive(Clone, Debug)]
pub struct ParquetWriterOptions {}

impl TryFrom<(&ConfigOptions, &StatementOptions)> for ParquetWriterOptions {
    type Error = DataFusionError;

    fn try_from(_: (&ConfigOptions, &StatementOptions)) -> Result<Self> {
        _not_impl_err!(
            "Parquet support is not enabled. Hint enable the `parquet` feature flag"
        )
    }
}
