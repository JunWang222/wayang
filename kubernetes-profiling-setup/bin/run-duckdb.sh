#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

"${SCRIPT_DIR}/setup-cluster.sh"
"${SCRIPT_DIR}/build-duckdb-image.sh"
"${SCRIPT_DIR}/submit-duckdb.sh"
"${SCRIPT_DIR}/verify-duckdb.sh"
