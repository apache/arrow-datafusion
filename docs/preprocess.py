#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import glob
import os
import re


def read_source(test_filename, test_method):
    lines = []
    with open(test_filename) as test:
        in_example_code = False
        for test_line in test.readlines():
            if test_line.strip() == "//begin:{}".format(test_method):
                in_example_code = True
                continue
            if test_line.strip() == "//end:{}".format(test_method):
                break
            if in_example_code:
                lines.append(test_line)

    # remove leading indent when possible
    consistent_indent = True
    for line in lines:
        if len(line.strip()) > 0 and not (
            line.startswith("    ") or line.startswith("\t")
        ):
            consistent_indent = False
            break
    if consistent_indent:
        old_lines = lines
        lines = []
        for line in old_lines:
            if len(line) >= 4:
                lines.append(line[4:])
            else:
                lines.append(line)

    return lines


def update_examples(source_file):
    print("Updating code samples in ", source_file)
    lines = []
    # finite state machine to track state
    state_scan = "scan"
    state_before_code = "before"
    state_in_code = "in"
    # <!-- include: library_logical_plan::plan_builder_1 -->
    include_pattern = "<!-- include: ([a-z0-9_]*)::([a-z0-9_]*) -->"
    with open(source_file, "r") as input:
        state = state_scan
        for line in input.readlines():
            if state == state_scan:
                lines.append(line)
                matches = re.search(include_pattern, line)
                if matches is not None:
                    state = state_before_code
                    test_file = matches.group(1)
                    test_method = matches.group(2)
                    test_filename = "src/{}.rs".format(test_file)
                    lines.append("\n```rust\n")
                    source = read_source(test_filename, test_method)
                    if len(source) == 0:
                        raise "failed to read source code from unit tests"
                    for x in source:
                        lines.append(x)
                    lines.append("```\n")
            elif state == state_before_code:
                # there can be blank lines between the include directive and the start of the code
                if len(line.strip()) > 0:
                    if line.startswith("```rust"):
                        state = state_in_code
                    else:
                        raise "expected Rust code to immediately follow include directive but found other content"
            elif state == state_in_code:
                if line.strip() == "```":
                    state = state_scan

    if state == state_scan:
        with open(source_file, "w") as output:
            for line in lines:
                output.write(line)
    else:
        raise "failed to rewrite example source code"


def main():
    for file in glob.glob("source/**/*.md"):
        update_examples(file)


if __name__ == "__main__":
    main()
