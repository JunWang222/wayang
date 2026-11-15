#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

set -euo pipefail

SETUP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPOSITORY_ROOT="$(cd "${SETUP_DIR}/.." && pwd)"
NAMESPACE="wayang-profiling"
PROFILE="${WAYANG_MINIKUBE_PROFILE:-wayang-profiling}"
DUCKDB_IMAGE="${WAYANG_DUCKDB_IMAGE:-wayang/duckdb-kubernetes-smoke:1.5.5.1}"

require_command() {
    if ! command -v "$1" >/dev/null 2>&1; then
        printf '%s\n' "Required command not found: $1" >&2
        return 1
    fi
}

find_minikube() {
    if command -v minikube >/dev/null 2>&1; then
        command -v minikube
    elif [[ -x "${SETUP_DIR}/.tools/minikube" ]]; then
        printf '%s\n' "${SETUP_DIR}/.tools/minikube"
    elif [[ -x "${REPOSITORY_ROOT}/spark-kubernetes-setup/.tools/minikube" ]]; then
        printf '%s\n' "${REPOSITORY_ROOT}/spark-kubernetes-setup/.tools/minikube"
    else
        printf '%s\n' "minikube was not found." >&2
        return 1
    fi
}
