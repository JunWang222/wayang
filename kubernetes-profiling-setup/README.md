<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
-->

# Reproducible Kubernetes profiling environments

This directory extends the existing Spark proof of concept with reproducible
platform-specific environments for DuckDB, Presto, and Trino. It is an
infrastructure and validation foundation; it does not define the paper's final
workloads or experiment matrix.

## Current status

| Platform | Kubernetes execution model | Status |
|---|---|---|
| Spark | Driver Pod and configurable executor Pods | Validated in `../spark-kubernetes-setup` |
| DuckDB | Embedded in one resource-limited Wayang Pod | Implemented here |
| Presto | Coordinator and configurable worker Pods | Implemented here |
| Trino | Coordinator and configurable worker Pods | Implemented here |

DuckDB is deliberately not deployed as a separate service. Wayang's DuckDB
platform uses the embedded JDBC driver, so the reproducible unit is the Wayang
Pod, its image, resource limits, database path, input size, and run metadata.

## DuckDB validation

Prerequisites: Docker, `kubectl`, Java 17, and minikube. Then run:

```bash
./kubernetes-profiling-setup/bin/run-duckdb.sh
```

The default validation creates 10,000 deterministic rows, executes a Wayang
`TableSource -> Filter -> TableSink` plan entirely on DuckDB, verifies the
result, and writes logs plus Kubernetes metadata under `artifacts/`.

The following environment variables control the run:

```bash
WAYANG_DUCKDB_ROWS=50000 \
WAYANG_DUCKDB_CPU_REQUEST=500m \
WAYANG_DUCKDB_CPU_LIMIT=1 \
WAYANG_DUCKDB_MEMORY_REQUEST=512Mi \
WAYANG_DUCKDB_MEMORY_LIMIT=1Gi \
./kubernetes-profiling-setup/bin/run-duckdb.sh
```

This smoke workload validates the deployment and Wayang integration only. The
existing `DuckDBCostPilotIT` should be used later when the team confirms the
profiling workload and repetition policy.

## Presto validation

The Presto setup starts one coordinator and two workers by default, then runs
the same deterministic Wayang smoke plan against the writable memory catalog:

```bash
./kubernetes-profiling-setup/bin/run-presto.sh
```

The worker count and per-role Kubernetes resources are configurable. For
example:

```bash
WAYANG_PRESTO_WORKERS=3 \
WAYANG_PRESTO_WORKER_CPU_LIMIT=2 \
WAYANG_PRESTO_WORKER_MEMORY_LIMIT=3Gi \
./kubernetes-profiling-setup/bin/run-presto.sh
```

This validates inter-Pod discovery, distributed execution, Wayang JDBC access,
and metadata capture. It is not a final performance experiment. The memory
catalog is intentionally ephemeral so every validation run creates its own
fixture.

## Trino validation

The Trino smoke environment uses the official pinned Trino image, one
coordinator, configurable workers, and the writable memory connector:

```bash
./kubernetes-profiling-setup/bin/run-trino.sh
```

Set `WAYANG_TRINO_WORKERS` and the corresponding
`WAYANG_TRINO_WORKER_*`/`WAYANG_TRINO_COORDINATOR_*` variables to change the
topology and resource limits. This validates the Kubernetes and Wayang
integration without introducing an external metastore. The existing full
`TrinoOperatorsIT` and `TrinoCostPilotIT` suites use Iceberg/Parquet, so an
Iceberg catalog and object-store profile should be added only when that is the
confirmed experiment storage model.
