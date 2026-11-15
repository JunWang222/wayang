#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

source "$(dirname "$0")/common.sh"

require_command kubectl

kubectl apply -f "${SETUP_DIR}/kubernetes/presto-cluster.yaml"
kubectl set image deployment/presto-coordinator presto="${PRESTO_IMAGE}" --namespace "${NAMESPACE}"
kubectl set image deployment/presto-worker presto="${PRESTO_IMAGE}" --namespace "${NAMESPACE}"
kubectl scale deployment/presto-worker \
    --replicas="${WAYANG_PRESTO_WORKERS:-2}" --namespace "${NAMESPACE}"
kubectl set resources deployment/presto-coordinator --namespace "${NAMESPACE}" \
    --requests="cpu=${WAYANG_PRESTO_COORDINATOR_CPU_REQUEST:-500m},memory=${WAYANG_PRESTO_COORDINATOR_MEMORY_REQUEST:-1Gi}" \
    --limits="cpu=${WAYANG_PRESTO_COORDINATOR_CPU_LIMIT:-1},memory=${WAYANG_PRESTO_COORDINATOR_MEMORY_LIMIT:-2Gi}"
kubectl set resources deployment/presto-worker --namespace "${NAMESPACE}" \
    --requests="cpu=${WAYANG_PRESTO_WORKER_CPU_REQUEST:-500m},memory=${WAYANG_PRESTO_WORKER_MEMORY_REQUEST:-1Gi}" \
    --limits="cpu=${WAYANG_PRESTO_WORKER_CPU_LIMIT:-1},memory=${WAYANG_PRESTO_WORKER_MEMORY_LIMIT:-2Gi}"
kubectl rollout status deployment/presto-coordinator --namespace "${NAMESPACE}" \
    --timeout="${WAYANG_K8S_TIMEOUT_SECONDS:-900}s"
kubectl rollout status deployment/presto-worker --namespace "${NAMESPACE}" \
    --timeout="${WAYANG_K8S_TIMEOUT_SECONDS:-900}s"

kubectl delete job presto-smoke --namespace "${NAMESPACE}" --ignore-not-found
kubectl apply -f "${SETUP_DIR}/kubernetes/presto-job.yaml"
kubectl set image job/presto-smoke presto-client="${PRESTO_CLIENT_IMAGE}" --namespace "${NAMESPACE}"
kubectl set env job/presto-smoke \
    ROW_COUNT="${WAYANG_PRESTO_ROWS:-10000}" \
    PRESTO_JDBC_URL="jdbc:presto://presto:8080/memory" \
    --namespace "${NAMESPACE}"
kubectl patch job presto-smoke --namespace "${NAMESPACE}" --type merge \
    --patch '{"spec":{"suspend":false}}'

printf '%s\n' "Submitted Presto validation Job with ${WAYANG_PRESTO_WORKERS:-2} workers."
