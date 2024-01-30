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

#[allow(unused_imports)]
use datafusion::common::not_impl_err;
#[cfg(feature = "parquet")]
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::{displayable, ExecutionPlan};
use std::collections::HashMap;
use substrait::proto::expression::MaskExpression;
use substrait::proto::extensions;
#[cfg(feature = "parquet")]
use substrait::proto::read_rel::local_files::file_or_files::ParquetReadOptions;
use substrait::proto::read_rel::local_files::file_or_files::{FileFormat, PathType};
use substrait::proto::read_rel::local_files::FileOrFiles;
use substrait::proto::read_rel::LocalFiles;
use substrait::proto::read_rel::ReadType;
use substrait::proto::rel::RelType;
use substrait::proto::ReadRel;
use substrait::proto::Rel;

/// Convert DataFusion ExecutionPlan to Substrait Rel
pub fn to_substrait_rel(
    plan: &dyn ExecutionPlan,
    _extension_info: &mut (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ),
) -> Result<Box<Rel>> {
    #[cfg(not(feature = "parquet"))]
    {
        not_impl_err!("Requires 'parquet' feature to be enabled.")
    }
    #[cfg(feature = "parquet")]
    {
        if let Some(scan) = plan.as_any().downcast_ref::<ParquetExec>() {
            let base_config = scan.base_config();
            let mut substrait_files = vec![];
            for (partition_index, files) in base_config.file_groups.iter().enumerate() {
                for file in files {
                    substrait_files.push(FileOrFiles {
                        partition_index: partition_index.try_into().unwrap(),
                        start: 0,
                        length: file.object_meta.size as u64,
                        path_type: Some(PathType::UriPath(
                            file.object_meta.location.as_ref().to_string(),
                        )),
                        file_format: Some(FileFormat::Parquet(ParquetReadOptions {})),
                    });
                }
            }

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: None,
                    filter: None,
                    best_effort_filter: None,
                    projection: Some(MaskExpression {
                        select: None,
                        maintain_singular_struct: false,
                    }),
                    advanced_extension: None,
                    read_type: Some(ReadType::LocalFiles(LocalFiles {
                        items: substrait_files,
                        advanced_extension: None,
                    })),
                }))),
            }))
        } else {
            Err(DataFusionError::Substrait(format!(
                "Unsupported plan in Substrait physical plan producer: {}",
                displayable(plan).one_line()
            )))
        }
    }
}
