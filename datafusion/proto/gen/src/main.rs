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

use std::path::Path;

type Error = Box<dyn std::error::Error>;
type Result<T, E = Error> = std::result::Result<T, E>;

fn main() -> Result<(), String> {
    let proto_dir = Path::new("datafusion/proto");
    let proto_path = Path::new("datafusion/proto/proto/datafusion.proto");
    let out_dir = Path::new("datafusion/proto/src");

    // proto definitions has to be there
    let descriptor_path = proto_dir.join("proto/proto_descriptor.bin");

    prost_build::Config::new()
        .file_descriptor_set_path(&descriptor_path)
        .out_dir(out_dir)
        .compile_well_known_types()
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&[proto_path], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let descriptor_set = std::fs::read(&descriptor_path)
        .unwrap_or_else(|e| panic!("Cannot read {:?}: {}", &descriptor_path, e));

    pbjson_build::Builder::new()
        .out_dir(out_dir)
        .register_descriptors(&descriptor_set)
        .unwrap_or_else(|e| {
            panic!("Cannot register descriptors {:?}: {}", &descriptor_set, e)
        })
        .build(&[".datafusion"])
        .map_err(|e| format!("pbjson compilation failed: {e}"))?;

    let prost = proto_dir.join("src/datafusion.rs");
    let pbjson = proto_dir.join("src/datafusion.serde.rs");
    let common_path = proto_dir.join("src/datafusion_common.rs");

    std::fs::copy(prost, proto_dir.join("src/generated/prost.rs")).unwrap();
    std::fs::copy(pbjson, proto_dir.join("src/generated/pbjson.rs")).unwrap();
    std::fs::copy(
        common_path,
        proto_dir.join("src/generated/datafusion_proto_common.rs"),
    )
    .unwrap();

    Ok(())
}
