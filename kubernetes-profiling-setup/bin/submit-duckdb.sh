#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

source "$(dirname "$0")/common.sh"

require_command kubectl

kubectl delete job duckdb-smoke --namespace "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${SETUP_DIR}/kubernetes/duckdb-job.yaml"
kubectl set image job/duckdb-smoke \
    duckdb="${DUCKDB_IMAGE}" \
    --namespace "${NAMESPACE}"
kubectl set env job/duckdb-smoke \
    ROW_COUNT="${WAYANG_DUCKDB_ROWS:-10000}" \
    --namespace "${NAMESPACE}"
kubectl set resources job/duckdb-smoke \
    --namespace "${NAMESPACE}" \
    --requests="cpu=${WAYANG_DUCKDB_CPU_REQUEST:-500m},memory=${WAYANG_DUCKDB_MEMORY_REQUEST:-512Mi}" \
    --limits="cpu=${WAYANG_DUCKDB_CPU_LIMIT:-1},memory=${WAYANG_DUCKDB_MEMORY_LIMIT:-1Gi}"
kubectl patch job duckdb-smoke \
    --namespace "${NAMESPACE}" \
    --type merge \
    --patch '{"spec":{"suspend":false}}'

printf '%s\n' "Submitted DuckDB validation Job."
