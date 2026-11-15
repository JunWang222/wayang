#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

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
kubectl apply -f "${SETUP_DIR}/kubernetes/base.yaml"

printf '%s\n' "Kubernetes profiling environment is ready."
kubectl get nodes
