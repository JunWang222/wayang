<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements. See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License. You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Spark on Kubernetes profiling foundation

This directory provides a reproducible local Kubernetes environment for Apache
Wayang's Spark operator profiling work. It includes both a standalone Spark
Join smoke test and an end-to-end run of Wayang's `SparkJoinOperator` profiler.

The smoke test creates one Spark driver pod and three executor pods, runs an
RDD Join that requires shuffle, prints aggregated Spark task metrics, and
copies the driver log and Spark event log into `artifacts/`. Event logs are
written to a namespace-scoped persistent volume so they remain available after
the driver pod completes.

The Wayang run builds the profiler from the current checkout, packages it in a
Java 17 Spark 3.5.7 image, and invokes
`org.apache.wayang.profiler.spark.Main join` with one driver and three
executors. Its driver log, profiler CSV, raw event log, and aggregated task
metrics are copied into `artifacts/latest-wayang/`.

## Architecture

```text
submitter Job
    |
    +-- Spark driver pod
            |-- Spark executor pod 1
            |-- Spark executor pod 2
            `-- Spark executor pod 3
```

Spark uses Kubernetes as its native cluster manager. There is no separate
long-running Spark master pod in this setup.

## Prerequisites

- Docker Desktop with the Docker daemon running
- `kubectl`
- `minikube`
- at least 4 CPUs and 6 GiB available to Docker Desktop

On macOS with Homebrew:

```bash
brew install minikube kubectl
```

For a repository-local minikube binary, place an executable at:

```text
spark-kubernetes-setup/.tools/minikube
```

The `.tools/` directory is ignored by Git.

## Run the complete smoke test

From the repository root:

```bash
spark-kubernetes-setup/bin/run-all.sh
```

The individual steps are also available:

```bash
spark-kubernetes-setup/bin/setup-cluster.sh
spark-kubernetes-setup/bin/build-image.sh
spark-kubernetes-setup/bin/submit.sh
spark-kubernetes-setup/bin/verify.sh
```

Expected result markers in `artifacts/latest/driver.log`:

```text
WAYANG_K8S_SMOKE_RESULT ...
WAYANG_K8S_METRICS_SUMMARY ...
```

The summary contains Spark-level shuffle read/write bytes, remote shuffle
read bytes, spills, executor runtime, and executor CPU time. The raw Spark
event log is saved under `artifacts/latest/spark-events/` for later analysis.

## Run the Wayang Join profiler

From the repository root, the complete end-to-end path is:

```bash
spark-kubernetes-setup/bin/run-wayang.sh
```

If the cluster is already running, the stages can be invoked separately:

```bash
spark-kubernetes-setup/bin/build-wayang-image.sh
spark-kubernetes-setup/bin/submit-wayang.sh
spark-kubernetes-setup/bin/verify-wayang.sh
```

The verifier requires `python3` and creates:

```text
artifacts/latest-wayang/
|-- driver.log
|-- metrics.json
`-- spark-events/
    `-- <Spark application event log>
```

`driver.log` contains the profiler result in its existing CSV format. Ganglia
collection is disabled in Kubernetes, so its `disk`, `network`, and `cpu`
columns use `-1` to mean unavailable. `metrics.json` replaces those legacy
host-level readings with Spark-native task metrics, including per-stage and
whole-application shuffle read/write bytes, remote reads, spills, executor
runtime, executor CPU time, GC time, and input/output bytes.

The whole-application aggregate includes input preparation and the profiler's
second action for output cardinality. Use the per-stage entries when isolating
the measured Join: stages named `mapToPair at SparkJoinOperator` write its
shuffle, and `foreach at BinaryOperatorProfiler` is the timed Join action.

## Configuration

The scripts accept the following environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `WAYANG_MINIKUBE_PROFILE` | `wayang-profiling` | Isolated minikube profile |
| `WAYANG_MINIKUBE_CPUS` | `4` | CPUs assigned to minikube |
| `WAYANG_MINIKUBE_MEMORY` | `6144` | Memory assigned to minikube, in MiB |
| `WAYANG_K8S_TIMEOUT_SECONDS` | `600` / `900` | Smoke / Wayang verification timeout |

The Spark version is intentionally pinned to 3.5.7 to match Wayang's current
root Maven configuration.

## Inspect a run

```bash
kubectl get pods -n wayang-profiling
kubectl logs -n wayang-profiling -l spark-role=driver
kubectl describe pods -n wayang-profiling
```

The committed RBAC role is namespace-scoped and grants the `spark` service
account only the Kubernetes resource operations needed to create, observe, and
clean up Spark driver/executor resources. A small reader pod mounts the event
log persistent volume read-only so the verification script can export logs
after the driver completes.

## Current scope and next step

This foundation validates Kubernetes scheduling, Spark's driver/executor
lifecycle, a real Wayang Join, and reproducible metric collection. For
Kubernetes compatibility, the profiler warm-up is disabled because it stops
the cluster-mode Spark context and removes the driver service before the
measured run. The next research step is a controlled experiment matrix over
input cardinality, key distribution, partitions, executor count, cores, and
memory, followed by extraction of operator-level cost-model features from the
per-stage metrics.
