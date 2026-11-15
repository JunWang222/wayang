#!/usr/bin/env python3

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

"""Record the environment and submitted workload for a reproducible run."""

import argparse
import datetime
import json
import platform
import shutil
import subprocess
from pathlib import Path


SETUP_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = SETUP_DIR.parent
SENSITIVE_KEY_PARTS = ("access.key", "password", "secret", "token")


def command(*args, parse_json=False):
    try:
        output = subprocess.check_output(
            args,
            cwd=REPOSITORY_ROOT,
            stderr=subprocess.STDOUT,
            text=True,
        ).strip()
        return json.loads(output) if parse_json else output
    except (FileNotFoundError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        return {"unavailable": str(error)} if parse_json else None


def git_metadata():
    status = command("git", "status", "--porcelain")
    return {
        "commit": command("git", "rev-parse", "HEAD"),
        "branch": command("git", "branch", "--show-current"),
        "dirty": bool(status),
    }


def minikube_metadata(profile_name):
    executable = shutil.which("minikube")
    repository_local = SETUP_DIR / ".tools" / "minikube"
    if executable is None and repository_local.is_file():
        executable = str(repository_local)
    if executable is None:
        return {"profile": profile_name, "version": None}
    return {
        "profile": profile_name,
        "version": command(executable, "version", "--short"),
    }


def pod_metadata(pod):
    container = pod.get("spec", {}).get("containers", [{}])[0]
    container_status = pod.get("status", {}).get("containerStatuses", [{}])[0]
    metadata = pod.get("metadata", {})
    return {
        "name": metadata.get("name"),
        "uid": metadata.get("uid"),
        "node": pod.get("spec", {}).get("nodeName"),
        "image": container.get("image"),
        "image_id": container_status.get("imageID"),
        "spark_version": metadata.get("labels", {}).get("spark-version"),
        "resources": container.get("resources", {}),
    }


def submission_metadata(job):
    container = (
        job.get("spec", {})
        .get("template", {})
        .get("spec", {})
        .get("containers", [{}])[0]
    )
    return {
        "job": job.get("metadata", {}).get("name"),
        "image": container.get("image"),
        "command": container.get("command", []),
        "arguments": [
            redact_argument(argument) for argument in container.get("args", [])
        ],
        "resources": container.get("resources", {}),
    }


def redact_argument(argument):
    if "=" not in argument:
        return argument
    key, _ = argument.split("=", 1)
    if any(part in key.lower() for part in SENSITIVE_KEY_PARTS):
        return f"{key}=[REDACTED_SECRET]"
    return argument


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--driver-pod", required=True)
    parser.add_argument("--submitter-job", required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--minikube-profile", required=True)
    args = parser.parse_args()

    pod = command(
        "kubectl", "get", "pod", args.driver_pod,
        "--namespace", args.namespace, "--output", "json",
        parse_json=True,
    )
    job = command(
        "kubectl", "get", "job", args.submitter_job,
        "--namespace", args.namespace, "--output", "json",
        parse_json=True,
    )

    metadata = {
        "schema_version": 1,
        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "workload": args.workload,
        "source": git_metadata(),
        "host": {
            "system": platform.system(),
            "machine": platform.machine(),
        },
        "kubernetes": {
            "context": command("kubectl", "config", "current-context"),
            "version": command("kubectl", "version", "--output", "json", parse_json=True),
            "namespace": args.namespace,
            "minikube": minikube_metadata(args.minikube_profile),
        },
        "driver": pod_metadata(pod),
        "submission": submission_metadata(job),
    }

    args.output.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
