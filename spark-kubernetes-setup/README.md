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

This directory provides a reproducible local Kubernetes environment for the
first stage of Apache Wayang's Spark operator profiling work. It deliberately
starts with a standalone Spark Join smoke test. Once the infrastructure is
stable, the same image and submission path can be extended with the Wayang
Spark profiler.

The smoke test creates one Spark driver pod and three executor pods, runs an
RDD Join that requires shuffle, prints aggregated Spark task metrics, and
copies the driver log and Spark event log into `artifacts/`. Event logs are
written to a namespace-scoped persistent volume so they remain available after
the driver pod completes.

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

## Configuration

The scripts accept the following environment variables:

| Variable | Default | Purpose |
|---|---:|---|
| `WAYANG_MINIKUBE_PROFILE` | `wayang-profiling` | Isolated minikube profile |
| `WAYANG_MINIKUBE_CPUS` | `4` | CPUs assigned to minikube |
| `WAYANG_MINIKUBE_MEMORY` | `6144` | Memory assigned to minikube, in MiB |
| `WAYANG_K8S_TIMEOUT_SECONDS` | `600` | Verification timeout |

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
lifecycle, shuffle execution, and metric collection. It does not yet package
or submit the Wayang profiler. The next step is to build a Wayang-aware Spark
image and run `org.apache.wayang.profiler.spark.Main join` through the same
submission path, then replace the profiler's legacy Ganglia dependency with
Spark event-log/task metrics where appropriate.
