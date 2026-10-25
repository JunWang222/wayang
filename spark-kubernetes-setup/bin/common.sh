#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAMESPACE="wayang-profiling"
PROFILE="${WAYANG_MINIKUBE_PROFILE:-wayang-profiling}"
IMAGE="wayang/spark-kubernetes-smoke:3.5.7"

find_minikube() {
    if command -v minikube >/dev/null 2>&1; then
        command -v minikube
    elif [[ -x "${SETUP_DIR}/.tools/minikube" ]]; then
        printf '%s\n' "${SETUP_DIR}/.tools/minikube"
    else
        printf '%s\n' "minikube was not found. See ${SETUP_DIR}/README.md." >&2
        return 1
    fi
}
require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf '%s\n' "Required command not found: $1" >&2
        return 1
    fi
}
