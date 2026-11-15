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
require_command python3

deadline=$((SECONDS + ${WAYANG_K8S_TIMEOUT_SECONDS:-900}))
driver_pod=""

while [[ -z "${driver_pod}" && ${SECONDS} -lt ${deadline} ]]; do
    driver_pod="$(kubectl get pods \
        --namespace "${NAMESPACE}" \
        --selector 'spark-role=driver,spark-app-name=wayang-profiler-join' \
        --sort-by=.metadata.creationTimestamp \
        --output jsonpath='{.items[-1:].metadata.name}' 2>/dev/null || true)"
    [[ -n "${driver_pod}" ]] || sleep 2
done

if [[ -z "${driver_pod}" ]]; then
    printf '%s\n' "No Wayang Spark driver pod appeared." >&2
    kubectl logs job/wayang-join-submit --namespace "${NAMESPACE}" || true
    exit 1
fi

printf '%s\n' "Wayang Spark driver pod: ${driver_pod}"

phase=""
while [[ "${phase}" != "Succeeded" && "${phase}" != "Failed" && ${SECONDS} -lt ${deadline} ]]; do
    phase="$(kubectl get pod "${driver_pod}" --namespace "${NAMESPACE}" --output jsonpath='{.status.phase}')"
    printf '%s\n' "Driver phase: ${phase}"
    [[ "${phase}" == "Succeeded" || "${phase}" == "Failed" ]] || sleep 5
done

kubectl logs "${driver_pod}" --namespace "${NAMESPACE}" | tee /tmp/wayang-profiler-k8s-driver.log

if [[ "${phase}" != "Succeeded" ]]; then
    printf '%s\n' "Wayang Spark driver did not succeed before the timeout." >&2
    exit 1
fi

if ! rg -q '^input_card_0,input_card_1,output_card,wallclock,disk,network,cpu,machines,cores_per_machine$' \
    /tmp/wayang-profiler-k8s-driver.log; then
    printf '%s\n' "Wayang profiler did not print its expected CSV header." >&2
    exit 1
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)"
artifact_dir="${SETUP_DIR}/artifacts/wayang-${run_id}"
mkdir -p "${artifact_dir}"
cp /tmp/wayang-profiler-k8s-driver.log "${artifact_dir}/driver.log"

app_id="$(kubectl get pod "${driver_pod}" \
    --namespace "${NAMESPACE}" \
    --output jsonpath='{.metadata.labels.spark-app-selector}')"
if [[ ! "${app_id}" =~ ^[A-Za-z0-9._-]+$ ]]; then
    printf '%s\n' "The Spark application ID is missing or invalid: ${app_id}" >&2
    exit 1
fi
event_log="$(kubectl exec \
    spark-event-log-reader \
    --namespace "${NAMESPACE}" \
    -- sh -c "ls -1 /var/spark-events/${app_id}* 2>/dev/null | head -n 1 | xargs basename")"
if [[ -z "${event_log}" ]]; then
    printf '%s\n' "No Spark event log was written for the Wayang profiler." >&2
    exit 1
fi
mkdir -p "${artifact_dir}/spark-events"
kubectl cp \
    "${NAMESPACE}/spark-event-log-reader:/var/spark-events/${event_log}" \
    "${artifact_dir}/spark-events/${event_log}" >/dev/null
"${SETUP_DIR}/bin/summarize-event-log.py" \
    "${artifact_dir}/spark-events/${event_log}" \
    > "${artifact_dir}/metrics.json"
"${SETUP_DIR}/bin/write-run-metadata.py" \
    --output "${artifact_dir}/run-metadata.json" \
    --namespace "${NAMESPACE}" \
    --driver-pod "${driver_pod}" \
    --submitter-job wayang-join-submit \
    --workload wayang-join-reference \
    --minikube-profile "${PROFILE}"
ln -sfn "wayang-${run_id}" "${SETUP_DIR}/artifacts/latest-wayang"

printf '%s\n' "Wayang Spark Join profiler passed."
printf '%s\n' "Artifacts: ${artifact_dir}"
rg '^(input_card_0|[0-9]+,[0-9]+,)' "${artifact_dir}/driver.log"
printf '%s\n' "Spark task metrics: ${artifact_dir}/metrics.json"
