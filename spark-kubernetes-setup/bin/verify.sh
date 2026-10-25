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

source "$(dirname "$0")/common.sh"

require_command kubectl

deadline=$((SECONDS + ${WAYANG_K8S_TIMEOUT_SECONDS:-600}))
driver_pod=""

while [[ -z "${driver_pod}" && ${SECONDS} -lt ${deadline} ]]; do
    driver_pod="$(kubectl get pods \
        --namespace "${NAMESPACE}" \
        --selector 'spark-role=driver' \
        --sort-by=.metadata.creationTimestamp \
        --output jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)"
    [[ -n "${driver_pod}" ]] || sleep 2
done

if [[ -z "${driver_pod}" ]]; then
    printf '%s\n' "No Spark driver pod appeared." >&2
    kubectl logs job/spark-join-submit --namespace "${NAMESPACE}" || true
    exit 1
fi

printf '%s\n' "Spark driver pod: ${driver_pod}"

phase=""
while [[ "${phase}" != "Succeeded" && "${phase}" != "Failed" && ${SECONDS} -lt ${deadline} ]]; do
    phase="$(kubectl get pod "${driver_pod}" --namespace "${NAMESPACE}" --output jsonpath='{.status.phase}')"
    printf '%s\n' "Driver phase: ${phase}"
    [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]] || sleep 5
done

kubectl get pods --namespace "${NAMESPACE}" --output wide
kubectl logs "${driver_pod}" --namespace "${NAMESPACE}" | tee /tmp/wayang-k8s-driver.log

if [[ "${phase}" != "Succeeded" ]]; then
    printf '%s\n' "Spark driver did not succeed before the timeout." >&2
    exit 1
fi

if ! rg -q '^WAYANG_K8S_SMOKE_RESULT ' /tmp/wayang-k8s-driver.log; then
    printf '%s\n' "The driver succeeded but did not print the smoke-test result marker." >&2
    exit 1
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
artifact_dir="${SETUP_DIR}/artifacts/${run_id}"
mkdir -p "${artifact_dir}"
cp /tmp/wayang-k8s-driver.log "${artifact_dir}/driver.log"
event_log="$(kubectl exec \
    spark-event-log-reader \
    --namespace "${NAMESPACE}" \
    -- sh -c 'ls -1t /var/spark-events | head -n 1')"
if [[ -z "${event_log}" ]]; then
    printf '%s\n' "No Spark event log was written to the persistent volume." >&2
    exit 1
fi
mkdir -p "${artifact_dir}/spark-events"
kubectl cp \
    "${NAMESPACE}/spark-event-log-reader:/var/spark-events/${event_log}" \
    "${artifact_dir}/spark-events/${event_log}" >/dev/null
ln -sfn "${run_id}" "${SETUP_DIR}/artifacts/latest"

printf '%s\n' "Spark Join smoke test passed."
printf '%s\n' "Artifacts: ${artifact_dir}"
rg '^WAYANG_K8S_(SMOKE_RESULT|METRICS_SUMMARY)' "${artifact_dir}/driver.log"
