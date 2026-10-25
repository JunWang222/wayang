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

"""Aggregate successful Spark task metrics from a Spark event log."""

import argparse
import json
from pathlib import Path


METRIC_KEYS = (
    "tasks",
    "executor_run_time_ms",
    "executor_cpu_time_ns",
    "jvm_gc_time_ms",
    "shuffle_read_bytes",
    "shuffle_remote_read_bytes",
    "shuffle_local_read_bytes",
    "shuffle_write_bytes",
    "memory_bytes_spilled",
    "disk_bytes_spilled",
    "input_bytes_read",
    "output_bytes_written",
)


def empty_metrics():
    return {key: 0 for key in METRIC_KEYS}


def task_metrics(event):
    metrics = event.get("Task Metrics", {})
    shuffle_read = metrics.get("Shuffle Read Metrics", {})
    shuffle_write = metrics.get("Shuffle Write Metrics", {})
    input_metrics = metrics.get("Input Metrics", {})
    output_metrics = metrics.get("Output Metrics", {})
    remote_read = shuffle_read.get("Remote Bytes Read", 0)
    local_read = shuffle_read.get("Local Bytes Read", 0)
    return {
        "tasks": 1,
        "executor_run_time_ms": metrics.get("Executor Run Time", 0),
        "executor_cpu_time_ns": metrics.get("Executor CPU Time", 0),
        "jvm_gc_time_ms": metrics.get("JVM GC Time", 0),
        "shuffle_read_bytes": remote_read + local_read,
        "shuffle_remote_read_bytes": remote_read,
        "shuffle_local_read_bytes": local_read,
        "shuffle_write_bytes": shuffle_write.get("Shuffle Bytes Written", 0),
        "memory_bytes_spilled": metrics.get("Memory Bytes Spilled", 0),
        "disk_bytes_spilled": metrics.get("Disk Bytes Spilled", 0),
        "input_bytes_read": input_metrics.get("Bytes Read", 0),
        "output_bytes_written": output_metrics.get("Bytes Written", 0),
    }


def add_metrics(destination, source):
    for key in METRIC_KEYS:
        destination[key] += source[key]


def summarize(path):
    application = {}
    stage_names = {}
    stages = {}
    aggregate = empty_metrics()

    with path.open("r", encoding="utf-8") as event_log:
        for line in event_log:
            event = json.loads(line)
            event_type = event.get("Event")
            if event_type == "SparkListenerApplicationStart":
                application = {
                    "name": event.get("App Name"),
                    "id": event.get("App ID"),
                    "start_time_ms": event.get("Timestamp"),
                }
            elif event_type == "SparkListenerApplicationEnd":
                application["end_time_ms"] = event.get("Timestamp")
            elif event_type == "SparkListenerStageSubmitted":
                info = event.get("Stage Info", {})
                key = (info.get("Stage ID"), info.get("Stage Attempt ID", 0))
                stage_names[key] = info.get("Stage Name")
            elif event_type == "SparkListenerTaskEnd":
                task_info = event.get("Task Info", {})
                reason = event.get("Task End Reason", {}).get("Reason")
                if task_info.get("Failed") or reason != "Success":
                    continue
                key = (event.get("Stage ID"), event.get("Stage Attempt ID", 0))
                stage = stages.setdefault(key, empty_metrics())
                metrics = task_metrics(event)
                add_metrics(stage, metrics)
                add_metrics(aggregate, metrics)

    stage_summaries = []
    for (stage_id, attempt_id), metrics in sorted(stages.items()):
        stage_summaries.append({
            "stage_id": stage_id,
            "attempt_id": attempt_id,
            "name": stage_names.get((stage_id, attempt_id)),
            **metrics,
        })

    if "start_time_ms" in application and "end_time_ms" in application:
        application["duration_ms"] = application["end_time_ms"] - application["start_time_ms"]

    return {
        "application": application,
        "aggregate_successful_tasks": aggregate,
        "stages": stage_summaries,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("event_log", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.event_log), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
