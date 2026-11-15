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

require_command docker
require_command kubectl
MINIKUBE="$(find_minikube)"

docker info >/dev/null

if ! "${MINIKUBE}" status --profile "${PROFILE}" >/dev/null 2>&1; then
    "${MINIKUBE}" start \
        --profile "${PROFILE}" \
        --driver docker \
        --cpus "${WAYANG_MINIKUBE_CPUS:-4}" \
        --memory "${WAYANG_MINIKUBE_MEMORY:-6144}"
fi

"${MINIKUBE}" update-context --profile "${PROFILE}"
kubectl apply -f "${SETUP_DIR}/kubernetes/rbac.yaml"
kubectl apply -f "${SETUP_DIR}/kubernetes/event-log-storage.yaml"
kubectl wait \
    --for=condition=Ready \
    pod/spark-event-log-reader \
    --namespace "${NAMESPACE}" \
    --timeout 120s
kubectl auth can-i create pods --as "system:serviceaccount:${NAMESPACE}:spark" --namespace "${NAMESPACE}"
kubectl auth can-i deletecollection pods --as "system:serviceaccount:${NAMESPACE}:spark" --namespace "${NAMESPACE}"

printf '%s\n' "Kubernetes profiling environment is ready."
kubectl get nodes
