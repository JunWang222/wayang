<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0.
-->

# Kubernetes profiling setup

This directory provides repeatable local Kubernetes environments for
validating Wayang on DuckDB, Presto, and Trino. The existing Spark setup is in
`../spark-kubernetes-setup`.

The scripts build the required Wayang modules and client images, deploy the
selected platform to minikube, run a deterministic smoke workload, verify the
result, and save the run metadata. They provide the infrastructure for later
profiling work; they do not define the paper's final workloads or experiment
matrix.

## Supported platforms

| Platform | Deployment | Default topology |
|---|---|---|
| DuckDB | Embedded in the Wayang client Pod | One resource-limited Pod |
| Presto | Distributed PrestoDB cluster | One coordinator, two workers |
| Trino | Distributed Trino cluster | One coordinator, two workers |

DuckDB is embedded through its JDBC driver and therefore does not run as a
separate service. Presto and Trino use an ephemeral memory catalog for the
smoke workload. Their data is recreated on every run.

## Requirements

- Docker with at least 6 GB of memory available
- Java 17
- `kubectl`
- `minikube`
- Bash, Python 3, and ripgrep (`rg`)

Run all commands from the repository root. Make sure Docker is running before
starting a platform.

## Quick start

Check out the branch and choose one platform:

```bash
git checkout feature/multiplatform-kubernetes-profiling

./kubernetes-profiling-setup/bin/run-duckdb.sh
# or
./kubernetes-profiling-setup/bin/run-presto.sh
# or
./kubernetes-profiling-setup/bin/run-trino.sh
```

The first run takes longer because Maven and Docker dependencies must be
downloaded. The scripts create or reuse the minikube profile
`wayang-profiling` and the namespace `wayang-profiling`.

A successful run ends with one of these lines:

```text
WAYANG_K8S_DUCKDB_RESULT ... status=PASS
WAYANG_K8S_PRESTO_RESULT ... status=PASS
WAYANG_K8S_TRINO_RESULT ... status=PASS
```

The default smoke workload creates 10,000 deterministic rows and executes a
Wayang `TableSource -> Filter -> TableSink` plan. The expected output contains
3,334 rows.

## Running one step at a time

The `run-*.sh` scripts execute four steps. The individual scripts can be used
while debugging or changing a deployment:

```bash
./kubernetes-profiling-setup/bin/setup-cluster.sh
./kubernetes-profiling-setup/bin/build-presto-client-image.sh
./kubernetes-profiling-setup/bin/submit-presto.sh
./kubernetes-profiling-setup/bin/verify-presto.sh
```

Replace `presto` with `trino` for Trino. DuckDB uses
`build-duckdb-image.sh`, `submit-duckdb.sh`, and `verify-duckdb.sh`.

## Configuration

The minikube profile uses four CPUs and 6 GB of memory by default:

```bash
WAYANG_MINIKUBE_CPUS=6 \
WAYANG_MINIKUBE_MEMORY=8192 \
./kubernetes-profiling-setup/bin/run-presto.sh
```

DuckDB input size and Pod resources can be changed as follows:

```bash
WAYANG_DUCKDB_ROWS=50000 \
WAYANG_DUCKDB_CPU_REQUEST=500m \
WAYANG_DUCKDB_CPU_LIMIT=1 \
WAYANG_DUCKDB_MEMORY_REQUEST=512Mi \
WAYANG_DUCKDB_MEMORY_LIMIT=1Gi \
./kubernetes-profiling-setup/bin/run-duckdb.sh
```

Presto and Trino accept the same topology and resource settings with their
respective prefixes. For example:

```bash
WAYANG_PRESTO_ROWS=50000 \
WAYANG_PRESTO_WORKERS=3 \
WAYANG_PRESTO_COORDINATOR_CPU_LIMIT=1 \
WAYANG_PRESTO_COORDINATOR_MEMORY_LIMIT=2Gi \
WAYANG_PRESTO_WORKER_CPU_LIMIT=2 \
WAYANG_PRESTO_WORKER_MEMORY_LIMIT=3Gi \
./kubernetes-profiling-setup/bin/run-presto.sh
```

Use `WAYANG_TRINO_*` instead of `WAYANG_PRESTO_*` for Trino. CPU and memory
requests are also configurable by replacing `_LIMIT` with `_REQUEST`. The
default timeout is 900 seconds and can be changed with
`WAYANG_K8S_TIMEOUT_SECONDS`.

The platform images can be overridden with `WAYANG_DUCKDB_IMAGE`,
`WAYANG_PRESTO_IMAGE`, or `WAYANG_TRINO_IMAGE`. Keep image versions fixed when
comparing runs.

## Run artifacts

Each successful validation creates a timestamped directory under
`kubernetes-profiling-setup/artifacts/` and updates a `latest-<platform>`
symlink. For example:

```text
artifacts/presto-20261115T030212Z/
  job.log
  job.json
  pod.json
  deployments.json
  cluster-pods.json
  run-metadata.json
```

DuckDB does not produce the two cluster files because it runs in one Pod.
`run-metadata.json` records the Git commit and dirty state, image and image ID,
Kubernetes and minikube versions, Pod and node, resource requests and limits,
and workload settings. The artifacts are intentionally excluded from Git.

For a reproducible result, commit local changes before the final run and check
that `source.dirty` is `false` in `run-metadata.json`.

## Switching between Presto and Trino

A local minikube profile may not have enough memory to run both distributed
clusters at the same time. Scale down the current platform before starting the
other one:

```bash
kubectl scale deployment/presto-coordinator deployment/presto-worker \
  --replicas=0 --namespace wayang-profiling

kubectl scale deployment/trino-coordinator deployment/trino-worker \
  --replicas=0 --namespace wayang-profiling
```

The next `run-presto.sh` or `run-trino.sh` invocation restores the requested
topology.

## Troubleshooting

Check the namespace when a run does not complete:

```bash
kubectl get pods --namespace wayang-profiling
kubectl get jobs --namespace wayang-profiling
kubectl logs job/presto-smoke --namespace wayang-profiling
kubectl logs job/trino-smoke --namespace wayang-profiling
kubectl describe pod <pod-name> --namespace wayang-profiling
```

If Docker is unavailable, start Docker and rerun the command. If a Pod is
`Pending`, check the minikube CPU and memory allocation. If a previous Job
failed, rerunning `run-*.sh` replaces that Job and starts with a new fixture.

To stop the local cluster without deleting it:

```bash
minikube stop --profile wayang-profiling
```

To remove the complete local environment and its stored data:

```bash
minikube delete --profile wayang-profiling
```

## Scope

These smoke workloads confirm that the platform starts, Wayang can submit a
plan, distributed nodes communicate, the expected result is produced, and run
metadata is captured. The reported wall-clock values are validation output,
not benchmark results.

The final profiling workload, input sizes, repetition policy, and resource
metrics still need to be agreed by the project team. The full Trino operator
and cost-pilot tests use Iceberg and Parquet; an Iceberg catalog and object
store should be added only if that becomes the selected experiment storage
model.
