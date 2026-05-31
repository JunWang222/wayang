<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Local Presto setup

This setup starts a single-node Presto server with the `memory` catalog enabled.

Start Presto:

```powershell
cd presto-setup
docker compose up -d
```

Run the integration test, which creates `memory.sales.orders`, checks it once
through raw Presto JDBC, then executes the Wayang Presto plans:

```powershell
cd ..
.\mvnw.cmd test -pl wayang-platforms/wayang-generic-jdbc -Dtest=PrestoGenericJdbcIT -Drat.skip=true -Dmaven.javadoc.skip=true
```

The test uses:

```text
jdbc:presto://localhost:8080/memory/sales
```
