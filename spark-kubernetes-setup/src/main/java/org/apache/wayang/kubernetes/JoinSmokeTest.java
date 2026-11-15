/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.kubernetes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * A small, deterministic join that validates Spark shuffle execution on Kubernetes.
 */
public final class JoinSmokeTest {

    private JoinSmokeTest() {
    }

    public static void main(String[] args) {
        final long leftCardinality = parseLongArg(args, 0, 200_000L);
        final long rightCardinality = parseLongArg(args, 1, 150_000L);
        final long keyCardinality = parseLongArg(args, 2, 50_000L);
        final int partitions = Math.toIntExact(parseLongArg(args, 3, 12L));

        if (leftCardinality <= 0 || rightCardinality <= 0 || keyCardinality <= 0 || partitions <= 0) {
            throw new IllegalArgumentException("Cardinalities and partitions must all be positive.");
        }
        if (leftCardinality > Integer.MAX_VALUE || rightCardinality > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("The smoke test materializes input indices on the driver.");
        }

        final SparkConf sparkConf = new SparkConf().setAppName("wayang-spark-join-smoke");
        try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
            sparkContext.setLogLevel("WARN");

            final ShuffleMetricsListener metricsListener = new ShuffleMetricsListener();
            sparkContext.sc().addSparkListener(metricsListener);

            final JavaRDD<Long> left = sparkContext.parallelize(sequence(leftCardinality), partitions);
            final JavaRDD<Long> right = sparkContext.parallelize(sequence(rightCardinality), partitions);

            final JavaPairRDD<Long, Long> keyedLeft = left.mapToPair(
                    value -> new Tuple2<>(value % keyCardinality, value)
            );
            final JavaPairRDD<Long, Long> keyedRight = right.mapToPair(
                    value -> new Tuple2<>(value % keyCardinality, value)
            );

            final long startedAt = System.nanoTime();
            final long matchCount = keyedLeft.join(keyedRight, partitions).count();
            final long wallClockMillis = (System.nanoTime() - startedAt) / 1_000_000L;

            if (matchCount <= 0) {
                throw new IllegalStateException("Join unexpectedly produced no matches.");
            }

            System.out.printf(
                    "WAYANG_K8S_SMOKE_RESULT left=%d right=%d keys=%d partitions=%d matches=%d wallClockMs=%d%n",
                    leftCardinality,
                    rightCardinality,
                    keyCardinality,
                    partitions,
                    matchCount,
                    wallClockMillis
            );
            metricsListener.printSummary();
        }
    }

    private static List<Long> sequence(long cardinality) {
        return LongStream.range(0L, cardinality).boxed().collect(Collectors.toList());
    }

    private static long parseLongArg(String[] args, int index, long defaultValue) {
        return args.length > index ? Long.parseLong(args[index]) : defaultValue;
    }

    private static final class ShuffleMetricsListener extends SparkListener {

        private final AtomicLong completedStages = new AtomicLong();
        private final AtomicLong executorRunTimeMs = new AtomicLong();
        private final AtomicLong executorCpuTimeNs = new AtomicLong();
        private final AtomicLong shuffleReadBytes = new AtomicLong();
        private final AtomicLong shuffleRemoteReadBytes = new AtomicLong();
        private final AtomicLong shuffleWriteBytes = new AtomicLong();
        private final AtomicLong memoryBytesSpilled = new AtomicLong();
        private final AtomicLong diskBytesSpilled = new AtomicLong();

        @Override
        public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
            final StageInfo stageInfo = stageCompleted.stageInfo();
            final TaskMetrics metrics = stageInfo.taskMetrics();
            if (metrics == null) {
                return;
            }

            this.completedStages.incrementAndGet();
            this.executorRunTimeMs.addAndGet(metrics.executorRunTime());
            this.executorCpuTimeNs.addAndGet(metrics.executorCpuTime());
            this.shuffleReadBytes.addAndGet(metrics.shuffleReadMetrics().totalBytesRead());
            this.shuffleRemoteReadBytes.addAndGet(metrics.shuffleReadMetrics().remoteBytesRead());
            this.shuffleWriteBytes.addAndGet(metrics.shuffleWriteMetrics().bytesWritten());
            this.memoryBytesSpilled.addAndGet(metrics.memoryBytesSpilled());
            this.diskBytesSpilled.addAndGet(metrics.diskBytesSpilled());

            System.out.printf(
                    "WAYANG_K8S_STAGE_METRICS stageId=%d attempt=%d tasks=%d shuffleReadBytes=%d "
                            + "shuffleRemoteReadBytes=%d shuffleWriteBytes=%d memoryBytesSpilled=%d "
                            + "diskBytesSpilled=%d executorRunTimeMs=%d executorCpuTimeNs=%d%n",
                    stageInfo.stageId(),
                    stageInfo.attemptNumber(),
                    stageInfo.numTasks(),
                    metrics.shuffleReadMetrics().totalBytesRead(),
                    metrics.shuffleReadMetrics().remoteBytesRead(),
                    metrics.shuffleWriteMetrics().bytesWritten(),
                    metrics.memoryBytesSpilled(),
                    metrics.diskBytesSpilled(),
                    metrics.executorRunTime(),
                    metrics.executorCpuTime()
            );
        }

        private void printSummary() {
            System.out.printf(
                    "WAYANG_K8S_METRICS_SUMMARY stages=%d shuffleReadBytes=%d shuffleRemoteReadBytes=%d "
                            + "shuffleWriteBytes=%d memoryBytesSpilled=%d diskBytesSpilled=%d "
                            + "executorRunTimeMs=%d executorCpuTimeNs=%d%n",
                    this.completedStages.get(),
                    this.shuffleReadBytes.get(),
                    this.shuffleRemoteReadBytes.get(),
                    this.shuffleWriteBytes.get(),
                    this.memoryBytesSpilled.get(),
                    this.diskBytesSpilled.get(),
                    this.executorRunTimeMs.get(),
                    this.executorCpuTimeNs.get()
            );
        }
    }
}
