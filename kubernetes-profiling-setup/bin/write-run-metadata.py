#!/usr/bin/env python3

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Record source, image, resource, and workload metadata for a profiling run."""

import argparse
import datetime
import json
import platform
import shutil
import subprocess
from pathlib import Path


SETUP_DIR = Path(__file__).resolve().parent.parent
REPOSITORY_ROOT = SETUP_DIR.parent
SENSITIVE_KEY_PARTS = ("access_key", "password", "secret", "token")


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
    local_candidates = (
        SETUP_DIR / ".tools" / "minikube",
        REPOSITORY_ROOT / "spark-kubernetes-setup" / ".tools" / "minikube",
    )
    if executable is None:
        executable = next((str(path) for path in local_candidates if path.is_file()), None)
    return {
        "profile": profile_name,
        "version": command(executable, "version", "--short") if executable else None,
    }


def redacted_environment(container):
    environment = {}
    for item in container.get("env", []):
        name = item.get("name")
        if not name:
            continue
        if any(part in name.lower() for part in SENSITIVE_KEY_PARTS):
            environment[name] = "[REDACTED_SECRET]"
        elif "value" in item:
            environment[name] = item["value"]
        else:
            environment[name] = {"valueFrom": True}
    return environment


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--pod", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--workload", required=True)
    parser.add_argument("--minikube-profile", required=True)
    args = parser.parse_args()

    job = command(
        "kubectl", "get", "job", args.job,
        "--namespace", args.namespace, "--output", "json", parse_json=True,
    )
    pod = command(
        "kubectl", "get", "pod", args.pod,
        "--namespace", args.namespace, "--output", "json", parse_json=True,
    )
    container = pod.get("spec", {}).get("containers", [{}])[0]
    container_status = pod.get("status", {}).get("containerStatuses", [{}])[0]

    metadata = {
        "schema_version": 1,
        "captured_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "platform": args.platform,
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
        "execution": {
            "job": job.get("metadata", {}).get("name"),
            "pod": pod.get("metadata", {}).get("name"),
            "pod_uid": pod.get("metadata", {}).get("uid"),
            "node": pod.get("spec", {}).get("nodeName"),
            "image": container.get("image"),
            "image_id": container_status.get("imageID"),
            "resources": container.get("resources", {}),
            "environment": redacted_environment(container),
        },
    }
    args.output.write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
