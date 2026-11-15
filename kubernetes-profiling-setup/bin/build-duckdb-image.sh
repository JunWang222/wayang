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
LIB_DIR="${SETUP_DIR}/.build/duckdb-libs"
CLASS_DIR="${SETUP_DIR}/.build/duckdb-classes"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-platforms/wayang-duckdb \
    --also-make \
    --define skipTests \
    --define rat.skip=true \
    install

rm -rf "${LIB_DIR}" "${CLASS_DIR}"
mkdir -p "${LIB_DIR}" "${CLASS_DIR}"

"${REPOSITORY_ROOT}/mvnw" \
    --projects wayang-platforms/wayang-duckdb \
    --define skipTests \
    dependency:copy-dependencies \
    --define includeScope=runtime \
    --define outputDirectory="${LIB_DIR}"

duckdb_jar="$(find "${REPOSITORY_ROOT}/wayang-platforms/wayang-duckdb/target" \
    -maxdepth 1 -type f -name 'apache-wayang-duckdb-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' -print -quit)"
if [[ -z "${duckdb_jar}" ]]; then
    printf '%s\n' "Could not find the built Wayang DuckDB JAR." >&2
    exit 1
fi
cp "${duckdb_jar}" "${LIB_DIR}/wayang-duckdb.jar"

javac \
    --release 17 \
    --class-path "${LIB_DIR}/*" \
    -d "${CLASS_DIR}" \
    "${SETUP_DIR}/src/main/java/org/apache/wayang/kubernetes/duckdb/DuckDBSmokeTest.java"
jar --create --file "${SETUP_DIR}/.build/duckdb-smoke.jar" -C "${CLASS_DIR}" .

docker build \
    --file "${SETUP_DIR}/Dockerfile.duckdb" \
    --tag "${DUCKDB_IMAGE}" \
    "${SETUP_DIR}"
"${MINIKUBE}" image load --profile "${PROFILE}" "${DUCKDB_IMAGE}"

printf '%s\n' "Loaded ${DUCKDB_IMAGE} into minikube profile ${PROFILE}."
