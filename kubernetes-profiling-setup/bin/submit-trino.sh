#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

source "$(dirname "$0")/common.sh"

require_command kubectl

kubectl apply -f "${SETUP_DIR}/kubernetes/trino-cluster.yaml"
kubectl set image deployment/trino-coordinator trino="${TRINO_IMAGE}" --namespace "${NAMESPACE}"
kubectl set image deployment/trino-worker trino="${TRINO_IMAGE}" --namespace "${NAMESPACE}"
kubectl scale deployment/trino-worker \
    --replicas="${WAYANG_TRINO_WORKERS:-2}" --namespace "${NAMESPACE}"
kubectl set resources deployment/trino-coordinator --namespace "${NAMESPACE}" \
    --requests="cpu=${WAYANG_TRINO_COORDINATOR_CPU_REQUEST:-500m},memory=${WAYANG_TRINO_COORDINATOR_MEMORY_REQUEST:-1Gi}" \
    --limits="cpu=${WAYANG_TRINO_COORDINATOR_CPU_LIMIT:-1},memory=${WAYANG_TRINO_COORDINATOR_MEMORY_LIMIT:-2Gi}"
kubectl set resources deployment/trino-worker --namespace "${NAMESPACE}" \
    --requests="cpu=${WAYANG_TRINO_WORKER_CPU_REQUEST:-500m},memory=${WAYANG_TRINO_WORKER_MEMORY_REQUEST:-1Gi}" \
    --limits="cpu=${WAYANG_TRINO_WORKER_CPU_LIMIT:-1},memory=${WAYANG_TRINO_WORKER_MEMORY_LIMIT:-2Gi}"
kubectl rollout restart deployment/trino-worker --namespace "${NAMESPACE}"
kubectl rollout status deployment/trino-worker --namespace "${NAMESPACE}" \
    --timeout="${WAYANG_K8S_TIMEOUT_SECONDS:-900}s"
kubectl rollout restart deployment/trino-coordinator --namespace "${NAMESPACE}"
kubectl rollout status deployment/trino-coordinator --namespace "${NAMESPACE}" \
    --timeout="${WAYANG_K8S_TIMEOUT_SECONDS:-900}s"

kubectl delete job trino-smoke --namespace "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${SETUP_DIR}/kubernetes/trino-job.yaml"
kubectl set image job/trino-smoke trino-client="${TRINO_CLIENT_IMAGE}" --namespace "${NAMESPACE}"
kubectl set env job/trino-smoke \
    ROW_COUNT="${WAYANG_TRINO_ROWS:-10000}" \
    TRINO_JDBC_URL="jdbc:trino://trino:8080/memory" \
    --namespace "${NAMESPACE}"
kubectl patch job trino-smoke --namespace "${NAMESPACE}" --type merge \
    --patch '{"spec":{"suspend":false}}'

printf '%s\n' "Submitted Trino validation Job with ${WAYANG_TRINO_WORKERS:-2} workers."

