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

FROM apache/spark:3.5.7 AS spark-runtime

FROM eclipse-temurin:11-jdk AS builder

WORKDIR /build

COPY --from=spark-runtime /opt/spark/jars /opt/spark/jars
COPY src/main/java/org/apache/wayang/kubernetes/JoinSmokeTest.java /build/JoinSmokeTest.java

RUN mkdir -p /build/classes \
    && javac -cp '/opt/spark/jars/*' -d /build/classes /build/JoinSmokeTest.java \
    && jar --create \
        --file /build/wayang-spark-kubernetes-smoke.jar \
        -C /build/classes .

FROM spark-runtime

USER root
COPY --from=builder \
    /build/wayang-spark-kubernetes-smoke.jar \
    /opt/spark/app/wayang-spark-kubernetes-smoke.jar
RUN chown 185:0 /opt/spark/app/wayang-spark-kubernetes-smoke.jar
USER 185
