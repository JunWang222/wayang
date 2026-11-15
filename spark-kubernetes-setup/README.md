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

# Reproducible Spark experiments on Kubernetes

This directory provides a reusable local Kubernetes foundation for Spark and
Apache Wayang experiments. Its purpose is to make the deployment topology,
software versions, resource settings, submission path, and collected artifacts
repeatable. It does not prescribe the workloads or experiment matrix for a
particular study.

Two Join workloads are included only as reference validations. They exercise
multiple executors, shuffle, inter-pod communication, persistent event logs,
and metric extraction. Future experiments can replace them with another Spark
or Wayang application while reusing the same namespace, RBAC, storage, and
artifact-collection pattern.

## Platform components

- An isolated `wayang-profiling` namespace.
- Namespace-scoped RBAC for Spark driver and executor lifecycle operations.
- Persistent Spark event-log storage and a read-only log-export pod.
- Pinned Spark 3.5.7 images, with Java 17 for the Wayang runtime.
- Cluster-mode Spark submission with explicit driver and executor resources.
- Export of driver logs, raw Spark event logs, task metrics, and run metadata.

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

## Reference workload 1: standalone Spark Join

This is an infrastructure smoke test, not a prescribed paper experiment. It
uses a small standalone Spark application to verify the Kubernetes path without
depending on Wayang.

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
event log is saved under `artifacts/latest/spark-events/` for later analysis,
and `artifacts/latest/run-metadata.json` records the environment and submitted
command.

## Reference workload 2: Wayang Join profiler

This validation demonstrates that an application built from the current
Wayang checkout can use the same Kubernetes foundation. Join was selected
because it exercises distributed shuffle; it is not the only workload that
the platform is intended to support.

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
|-- run-metadata.json
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

Both reference verifiers create `run-metadata.json`. It records the workload,
Git commit and dirty state, host architecture, Kubernetes context and version,
minikube profile and version, driver image ID, Spark version label, container
resources, and the complete submitted command. This provides the provenance
needed to reproduce a run without assuming that timing will be identical.

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

## Add another workload

A new workload does not require changing the base cluster. Use the existing
submitter Jobs as references and provide:

1. A versioned container image containing the application and its runtime.
2. A Kubernetes Job that invokes `spark-submit` in cluster mode.
3. Explicit driver, executor, memory, core, and partition settings.
4. The `spark-event-logs` PVC mounted in the driver pod.
5. A unique application label so its driver can be selected reliably.
6. A verifier that exports the driver log and event log and invokes
   `write-run-metadata.py`; use `summarize-event-log.py` when Spark task metrics
   are required.

The application class, arguments, data, and experiment parameters belong to
the workload definition. They are intentionally not imposed by the Kubernetes
foundation.

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

## Reproducibility scope and limitations

The committed manifests and scripts reproduce the software topology and record
the configuration of each run. Workload inputs and random seeds must also be
fixed by each experiment when deterministic data is required.

Minikube runs all pods on one physical node. It is appropriate for validating
deployment, logical shuffle volume, metric collection, and repeatability of the
workflow. Its wall-clock and network behavior must not be presented as
representative of a physical multi-node cluster. The paper team should define
the final workloads, input distributions, resource matrix, repetition policy,
and target cluster before collecting study results.

For Kubernetes compatibility, the Wayang reference workload disables the
profiler warm-up because that warm-up stops the cluster-mode Spark context and
removes the driver service before the measured run. This is specific to the
reference workload and does not alter the base Kubernetes environment.
