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
MINIKUBE="$(find_minikube)"
REPOSITORY_ROOT="$(cd "${SETUP_DIR}/.." && pwd)"
LIB_DIR="${SETUP_DIR}/.build/wayang-libs"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-profiler \
    --also-make \
    --define skipTests \
    --define rat.skip=true \
    install

rm -rf "${LIB_DIR}"
mkdir -p "${LIB_DIR}"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-profiler \
    --define skipTests \
    dependency:copy-dependencies \
    --define includeScope=runtime \
    --define excludeGroupIds=org.apache.spark,org.scala-lang,com.fasterxml.jackson.core,com.fasterxml.jackson.dataformat,com.fasterxml.jackson.datatype,com.fasterxml.jackson.jaxrs,com.fasterxml.jackson.module,org.slf4j,org.apache.logging.log4j \
    --define outputDirectory="${LIB_DIR}"

profiler_jar="$(find "${REPOSITORY_ROOT}/wayang-profiler/target" \
    -maxdepth 1 \
    -type f \
    -name 'apache-wayang-profiler-*.jar' \
    ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' \
    -print \
    -quit)"
if [[ -z "${profiler_jar}" ]]; then
    printf '%s\n' "Could not find the built Wayang profiler JAR." >&2
    exit 1
fi
cp "${profiler_jar}" "${LIB_DIR}/wayang-profiler.jar"

docker build \
    --file "${SETUP_DIR}/Dockerfile.wayang" \
    --tag "${WAYANG_IMAGE}" \
    "${SETUP_DIR}"
"${MINIKUBE}" image load --profile "${PROFILE}" "${WAYANG_IMAGE}"

printf '%s\n' "Loaded ${WAYANG_IMAGE} into minikube profile ${PROFILE}."
