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

# DataFusion Documentation

This folder contains the source content of the following guides:

- [User Guide]
- [Library Guide]
- [Contributor Guide]

These guides are published to https://arrow.apache.org/datafusion/ as part of the release process.

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python virtualenv.

Install Python and then use pip to install dependencies:

```shell
pip install -r requirements.txt
```

## Build & Preview

Run the provided script to build the HTML pages.

```shell
./build.sh
```

The HTML will be generated into a `build` directory.

Preview the site on Linux by running this command.

```shell
firefox build/html/index.html
```

## Making Changes

To make changes to the docs, simply make a Pull Request with your
proposed changes as normal. When the PR is merged the docs will be
automatically updated.

## Including Source Code

We want to make sure that all source code in the documentation is tested as part of the build and release process. We
achieve this by writing the code in standard Rust tests in the `datafusion-docs-test` crate, and annotate the code with
comments that mark the beginning and end of the portion of the code that we want to include in the documentation.

```rust
//begin:my_example
let foo = 1 + 1;
//end:my_example
```

We can now put an `include` directive in the markdown file, specifying the name of the Rust file containing the test
and the name of the example. The include directive must be followed immediately by a code block starting with  
`&#96;&#96;&#96;rust and ending with &#96;&#96;&#96;. This code block will be replaced whenever the `preprocess.py`
Python script is executed. Note that this action can potentially be destructive so be sure to commit documentation
changes or otherwise back them up before running this script.

```md
<!-- include: my_rust_file::my_example -->

&#96;&#96;&#96;rust
&#96;&#96;&#96;
```

## Release Process

This documentation is hosted at https://arrow.apache.org/datafusion/

When the PR is merged to the `main` branch of the DataFusion
repository, a [github workflow](https://github.com/apache/arrow-datafusion/blob/main/.github/workflows/docs.yaml) which:

1. Builds the html content
2. Pushes the html content to the [`asf-site`](https://github.com/apache/arrow-datafusion/tree/asf-site) branch in this repository.

The Apache Software Foundation provides https://arrow.apache.org/,
which serves content based on the configuration in
[.asf.yaml](https://github.com/apache/arrow-datafusion/blob/main/.asf.yaml),
which specifies the target as https://arrow.apache.org/datafusion/.

[user guide]: ./source/user-guide
[library guide]: ./source/library-user-guide
[contributor guide]: ./source/contributor-guide
