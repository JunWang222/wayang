#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

source "$(dirname "$0")/common.sh"

require_command docker
require_command javac
require_command jar
MINIKUBE="$(find_minikube)"
LIB_DIR="${SETUP_DIR}/.build/presto-libs"
CLASS_DIR="${SETUP_DIR}/.build/presto-classes"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-platforms/wayang-presto \
    --also-make \
    --define maven.test.skip=true \
    --define rat.skip=true \
    install

rm -rf "${LIB_DIR}" "${CLASS_DIR}"
mkdir -p "${LIB_DIR}" "${CLASS_DIR}"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-platforms/wayang-presto \
    --define maven.test.skip=true \
    dependency:copy-dependencies \
    --define includeScope=runtime \
    --define outputDirectory="${LIB_DIR}"

presto_jar="$(find "${REPOSITORY_ROOT}/wayang-platforms/wayang-presto/target" \
    -maxdepth 1 -type f -name 'apache-wayang-presto-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' -print -quit)"
if [[ -z "${presto_jar}" ]]; then
    printf '%s\n' "Could not find the built Wayang Presto JAR." >&2
    exit 1
fi
cp "${presto_jar}" "${LIB_DIR}/wayang-presto.jar"

javac \
    --release 17 \
    --class-path "${LIB_DIR}/*" \
    -d "${CLASS_DIR}" \
    "${SETUP_DIR}/src/main/java/org/apache/wayang/kubernetes/presto/PrestoSmokeTest.java"
jar --create --file "${SETUP_DIR}/.build/presto-smoke.jar" -C "${CLASS_DIR}" .

docker build \
    --file "${SETUP_DIR}/Dockerfile.presto" \
    --tag "${PRESTO_CLIENT_IMAGE}" \
    "${SETUP_DIR}"
"${MINIKUBE}" image load --profile "${PROFILE}" "${PRESTO_CLIENT_IMAGE}"
"${MINIKUBE}" image load --profile "${PROFILE}" "${PRESTO_IMAGE}"

printf '%s\n' "Loaded ${PRESTO_IMAGE} and ${PRESTO_CLIENT_IMAGE} into minikube profile ${PROFILE}."
