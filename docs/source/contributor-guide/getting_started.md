<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Getting Started

This section describes how you can get started at developing DataFusion.

## Windows setup

```shell
wget https://az792536.vo.msecnd.net/vms/VMBuild_20190311/VirtualBox/MSEdge/MSEdge.Win10.VirtualBox.zip
choco install -y git rustup.install visualcpp-build-tools
git-bash.exe
cargo build
```

## Protoc Installation

Compiling DataFusion from sources requires an installed version of the protobuf compiler, `protoc`.

On most platforms this can be installed from your system's package manager

```
# Ubuntu
$ sudo apt install -y protobuf-compiler

# Fedora
$ dnf install -y protobuf-devel

# Arch Linux
$ pacman -S protobuf

# macOS
$ brew install protobuf
```

You will want to verify the version installed is `3.15` or greater, which has support for explicit [field presence](https://github.com/protocolbuffers/protobuf/blob/v3.15.0/docs/field_presence.md). Older versions may fail to compile.

```shell
$ protoc --version
libprotoc 3.15.0
```

Alternatively a binary release can be downloaded from the [Release Page](https://github.com/protocolbuffers/protobuf/releases) or [built from source](https://github.com/protocolbuffers/protobuf/blob/main/src/README.md).

## Bootstrap environment

DataFusion is written in Rust and it uses a standard rust toolkit:

- `cargo build`
- `cargo fmt` to format the code
- `cargo test` to test
- etc.

Note that running `cargo test` requires significant memory resources, due to cargo running many tests in parallel by default. If you run into issues with slow tests or system lock ups, you can significantly reduce the memory required by instead running `cargo test -- --test-threads=1`. For more information see [this issue](https://github.com/apache/datafusion/issues/5347).

Testing setup:

- `rustup update stable` DataFusion uses the latest stable release of rust
- `git submodule init`
- `git submodule update`

Formatting instructions:

- [ci/scripts/rust_fmt.sh](../../../ci/scripts/rust_fmt.sh)
- [ci/scripts/rust_clippy.sh](../../../ci/scripts/rust_clippy.sh)
- [ci/scripts/rust_toml_fmt.sh](../../../ci/scripts/rust_toml_fmt.sh)

or run them all at once:

- [dev/rust_lint.sh](../../../dev/rust_lint.sh)
