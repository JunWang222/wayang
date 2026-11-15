#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

source "$(dirname "$0")/common.sh"

require_command kubectl
require_command python3
require_command rg

deadline=$((SECONDS + ${WAYANG_K8S_TIMEOUT_SECONDS:-900}))
job_state="Running"
while [[ ${SECONDS} -lt ${deadline} ]]; do
    if [[ "$(kubectl get job duckdb-smoke --namespace "${NAMESPACE}" \
        --output jsonpath='{.status.conditions[?(@.type=="Complete")].status}')" == "True" ]]; then
        job_state="Complete"
        break
    fi
    if [[ "$(kubectl get job duckdb-smoke --namespace "${NAMESPACE}" \
        --output jsonpath='{.status.conditions[?(@.type=="Failed")].status}')" == "True" ]]; then
        job_state="Failed"
        break
    fi
    sleep 2
done

if [[ "${job_state}" != "Complete" ]]; then
    printf '%s\n' "DuckDB Job ended in state ${job_state}." >&2
    kubectl describe job duckdb-smoke --namespace "${NAMESPACE}" || true
    kubectl logs job/duckdb-smoke --namespace "${NAMESPACE}" || true
    exit 1
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
artifact_dir="${SETUP_DIR}/artifacts/duckdb-${run_id}"
mkdir -p "${artifact_dir}"

kubectl logs job/duckdb-smoke --namespace "${NAMESPACE}" \
    | tee "${artifact_dir}/job.log"
kubectl get job duckdb-smoke --namespace "${NAMESPACE}" --output json \
    > "${artifact_dir}/job.json"
pod_name="$(kubectl get pods \
    --namespace "${NAMESPACE}" \
    --selector job-name=duckdb-smoke \
    --output jsonpath='{.items[0].metadata.name}')"
kubectl get pod "${pod_name}" --namespace "${NAMESPACE}" --output json \
    > "${artifact_dir}/pod.json"
"${SETUP_DIR}/bin/write-run-metadata.py" \
    --output "${artifact_dir}/run-metadata.json" \
    --namespace "${NAMESPACE}" \
    --job duckdb-smoke \
    --pod "${pod_name}" \
    --platform duckdb \
    --workload table-source-filter-table-sink \
    --minikube-profile "${PROFILE}"

if ! rg -q '^WAYANG_K8S_DUCKDB_RESULT .*status=PASS$' "${artifact_dir}/job.log"; then
    printf '%s\n' "DuckDB validation did not emit a passing result." >&2
    exit 1
fi

ln -sfn "duckdb-${run_id}" "${SETUP_DIR}/artifacts/latest-duckdb"
printf '%s\n' "Wayang DuckDB Kubernetes validation passed."
printf '%s\n' "Artifacts: ${artifact_dir}"
