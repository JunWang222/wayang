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

# JDBC engine-only integration tests

This branch combines the Trino, Presto, and BigQuery platform-only test work.
Each platform has integration coverage showing that a Wayang plan can run from
table source through table sink inside the SQL engine without registering
`Java.basicPlugin()`.

The shared executor change lives in `wayang-jdbc-template`. When a JDBC stage
ends in a `JdbcTableSinkOperator`, `JdbcExecutor.executeSinkStage(...)` composes
and runs `CREATE TABLE ... AS SELECT` directly on the engine connection. The
same path handles filter, projection, join, global reduce, reduce-by, and sort.

The generated SELECT queries intentionally omit trailing semicolons. They are
unnecessary for JDBC `executeQuery` and strict parsers such as Trino and BigQuery
reject them in some contexts; HSQLDB, Postgres, SQLite, and Presto accept their
absence.

The platform suites are:

- `TrinoOperatorsIT`
- `PrestoOperatorsIT`
- `BigQueryOperatorsIT`

Join tests still use a test-only named flatten mapping to bridge the logical
`Tuple2<Record, Record>` join output and the flat SQL result emitted by pushed
down JDBC joins.
