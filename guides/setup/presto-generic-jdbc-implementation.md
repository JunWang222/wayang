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

# Presto Generic JDBC Implementation Notes

This document explains the Presto local deployment, test data, Wayang connector
classes, and verification strategy on the `presto-jdbc` branch.

## Structure

```text
presto-setup/
├── docker-compose.yml
├── presto/catalog/memory.properties
├── scripts/init.sql
└── README.md

wayang-platforms/wayang-generic-jdbc/
├── src/main/java/org/apache/wayang/genericjdbc/Presto.java
├── src/main/java/org/apache/wayang/genericjdbc/presto/
├── src/main/resources/wayang-presto-defaults.properties
└── src/test/java/org/apache/wayang/genericjdbc/PrestoGenericJdbcIT.java
```

## Local Presto

`presto-setup/docker-compose.yml` starts:

```text
prestodb/presto:0.297
container: wayang-presto
port: 8080
```

`presto-setup/presto/catalog/memory.properties` enables:

```properties
connector.name=memory
```

The test table is:

```text
memory.sales.orders
```

Wayang uses:

```text
jdbc:presto://localhost:8080/memory/sales
```

and therefore table name `orders`.

## Test Data

The manual SQL copy is:

```text
presto-setup/scripts/init.sql
```

The automated test creates the same table and data in:

```text
PrestoGenericJdbcIT.checkPrestoAvailableAndLoadData()
```

Schema:

```text
order_id   INTEGER
region     VARCHAR
product    VARCHAR
amount     DOUBLE
order_date DATE
```

Expected counts:

```text
total rows: 20
APAC rows: 7
EMEA rows: 5
AMER rows: 6
amount > 1000 rows: 6
server and amount > 2000 rows: 2
```

## Connector Classes

`Presto.java` is the public entry point:

```java
Presto.plugin()
Presto.platform()
```

`PrestoPlatform.java` defines:

```text
platform: Presto
config namespace: presto
driver: com.facebook.presto.jdbc.PrestoDriver
```

This maps configuration to:

```text
wayang.presto.*
```

`PrestoPlugin.java` registers:

- `PrestoPlatform`
- `JavaPlatform`
- `PrestoFilterMapping`
- `PrestoProjectionMapping`
- `PrestoChannelConversions`

`PrestoTableSource.java` binds a table source to `PrestoPlatform`.

`PrestoFilterMapping.java` maps logical filters with SQL implementations to
`PrestoFilterOperator`.

`PrestoProjectionMapping.java` maps projection `MapOperator`s with
`ProjectionDescriptor` to `PrestoProjectionOperator`.

`PrestoChannelConversions.java` converts:

```text
Presto SqlQueryChannel -> Java StreamChannel
```

through `GenericSqlToStreamOperator`, which opens the Presto JDBC connection,
executes SQL, and converts `ResultSet` rows into Wayang `Record` objects.

`PrestoExecutor.java` currently reuses `GenericJdbcExecutor` SQL generation and
serves as the Presto-specific executor extension point.

## Generic JDBC Template Changes

`GenericJdbcPlatform` now has a protected constructor for engine-specific
platform/config names.

`GenericJdbcExecutor` and `GenericJdbcTableSource` no longer append trailing
semicolons to generated SQL. This avoids JDBC compatibility issues with Presto.

## Test Coverage

Run:

```powershell
.\mvnw.cmd test -pl wayang-platforms/wayang-generic-jdbc "-Dtest=PrestoGenericJdbcIT" "-Drat.skip=true" "-Dmaven.javadoc.skip=true"
```

Expected:

```text
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

Tests:

- `testPluginContributions`: plugin exposes platforms, mappings, conversions
- `testPlatformBinding`: physical Presto operator is bound to `PrestoPlatform`
- `testRawJdbcDataset`: direct JDBC `SELECT count(*) FROM orders` returns `20`
- `testWayangQueryAppearsInPrestoHistory`: Presto history records Wayang SQL
- `testTableScan`: direct JDBC rows equal Wayang rows
- `testFilterPushdown`: `region = 'APAC'`, expected `7` rows
- `testProjectionPushdown`: `region, amount`, expected `20` two-field rows
- `testFilterThenProjection`: `amount > 1000`, expected `6` rows

## Hard Proof That Wayang Uses Presto

The query-history test runs a Wayang plan expected to generate:

```sql
SELECT * FROM orders WHERE product = 'server' AND amount > 2000
```

It checks matching entries in:

```sql
system.runtime.queries
```

before and after the Wayang execution. The count must increase. This proves the
SQL reached Presto itself.

The result comparison tests also print:

```text
Direct Presto JDBC result
Wayang through Presto result
Comparison: sorted direct JDBC rows == sorted Wayang rows
```

So the Presto validation has two layers:

- Presto query history contains Wayang-generated SQL
- direct Presto JDBC results match Wayang-through-Presto results

## Execution Flow

```text
WayangContext
  + Java.basicPlugin()
  + Presto.plugin()
        |
        v
Logical Wayang plan
        |
        v
Presto mappings rewrite logical operators
        |
        v
GenericJdbcExecutor assembles SQL
        |
        v
SqlQueryChannel carries SQL and jdbcName = presto
        |
        v
GenericSqlToStreamOperator opens Presto JDBC connection
        |
        v
ResultSet rows become Wayang Record objects
        |
        v
LocalCallbackSink collects List<Record>
```
