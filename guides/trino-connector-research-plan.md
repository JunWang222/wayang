<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Research Plan: Trino Connector for Apache Wayang

## Problem

Apache Wayang currently supports JDBC-based databases (Postgres, SQLite) as
execution backends. Modern data lake environments typically use query engines
like **Trino** (formerly PrestoSQL) to query data stored in object storage
(S3, GCS) in open formats (Parquet, ORC, Iceberg, Delta).

Adding a Trino backend would let Wayang route data processing pipelines to
Trino, enabling hybrid pipelines like:
- Filter/aggregate large Parquet datasets in Trino
- Join the result with in-memory data in Java Streams
- All orchestrated automatically by Wayang's optimizer

---

## Why Trino is the Best Starting Point

Trino exposes a **standard JDBC driver** (`io.trino:trino-jdbc`). This means
~80% of the implementation is already handled by `wayang-jdbc-template`. The
Postgres connector is a near-identical template to follow.

| Engine | Has JDBC driver | New code needed |
|---|---|---|
| Trino | ✅ Yes | ~20% (SQL dialect, auth) |
| Apache Datafusion | ❌ No | ~100% (REST/Arrow Flight) |
| Dremio | ✅ Yes (partial) | ~40% (auth complexity) |
| BigQuery | ✅ Yes | ~30% (auth, cost model) |

---

## High-Level Implementation Plan

### Phase 1 — Setup & Local Trino Environment (Week 1)

Get Trino running locally with Docker and verify the JDBC driver works before
writing any Wayang code.

### Phase 2 — Scaffold the Module (Week 1-2)

Create `wayang-platforms/wayang-trino` by copying the Postgres module structure
and updating names. Get it to compile.

### Phase 3 — Wire Up Core Classes (Week 2-3)

Implement `TrinoPlatform`, `TrinoPlugin`, mappings, and operators following the
Postgres pattern exactly.

### Phase 4 — SQL Dialect Adjustments (Week 3-4)

Identify and fix Trino-specific SQL differences (type names, functions, etc.).

### Phase 5 — Testing & Demo (Week 4-5)

Write integration tests against a live Trino container and build a demo
pipeline.

---

## Where to Make Code Changes

### New module: `wayang-platforms/wayang-trino/`

Mirror the structure of `wayang-platforms/wayang-postgres/`:

```
wayang-platforms/wayang-trino/
├── pom.xml
└── src/main/java/org/apache/wayang/trino/
    ├── Trino.java                          ← entry point (like Postgres.java)
    ├── platform/
    │   └── TrinoPlatform.java              ← extends JdbcPlatformTemplate
    ├── plugin/
    │   ├── TrinoPlugin.java                ← registers mappings + platforms
    │   └── TrinoConversionsPlugin.java     ← registers channel conversions
    ├── mapping/
    │   ├── Mappings.java                   ← list of all mappings
    │   ├── FilterMapping.java              ← logical Filter → TrinoFilterOperator
    │   ├── ProjectionMapping.java
    │   └── JoinMapping.java
    └── operators/
        ├── TrinoExecutionOperator.java     ← marker interface (like PostgresExecutionOperator)
        ├── TrinoTableSource.java
        ├── TrinoFilterOperator.java
        ├── TrinoProjectionOperator.java
        └── TrinoJoinOperator.java
```

Also add `wayang-trino` as a submodule in the parent
`wayang-platforms/pom.xml`.

---

### File-by-File Change Guide

#### `TrinoPlatform.java`
Copy `PostgresPlatform.java`, change:
- Class name → `TrinoPlatform`
- `PLATFORM_NAME` → `"Trino"`
- `CONFIG_NAME` → `"trino"`
- `getJdbcDriverClassName()` → `"io.trino.jdbc.TrinoDriver"`

```java
public class TrinoPlatform extends JdbcPlatformTemplate {
    private static final String PLATFORM_NAME = "Trino";
    private static final String CONFIG_NAME = "trino";

    @Override
    public String getJdbcDriverClassName() {
        return "io.trino.jdbc.TrinoDriver";
    }
}
```

#### `wayang-trino-defaults.properties`
Create `src/main/resources/wayang-trino-defaults.properties`:

```properties
wayang.trino.cpu.mhz = 2700
wayang.trino.cores = 1
wayang.trino.costs.fix = 0
wayang.trino.costs.per-ms = 1
# JDBC URL format: jdbc:trino://host:port/catalog/schema
wayang.trino.jdbc.url = jdbc:trino://localhost:8080/hive/default
wayang.trino.jdbc.user = trino
wayang.trino.jdbc.password =
```

#### `pom.xml` (Trino module)
Key dependency to add:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-jdbc</artifactId>
    <version>435</version>  <!-- use latest stable -->
</dependency>
```

#### Operator classes
Each operator (`TrinoFilterOperator`, `TrinoProjectionOperator`, `TrinoTableSource`)
is a copy of the Postgres equivalent with class names updated. No SQL logic
changes needed at first — Trino supports standard SQL.

#### Mapping classes
Copy `FilterMapping.java`, `ProjectionMapping.java`, `JoinMapping.java` from
Postgres. Only change the target operator class names
(`PostgresFilterOperator` → `TrinoFilterOperator`) and the platform reference.

---

## Trino-Specific Gotchas

### 1. JDBC URL format
Trino JDBC URLs look different from Postgres:

```
# Postgres
jdbc:postgresql://localhost:5432/mydb

# Trino
jdbc:trino://localhost:8080/hive/default
#                          ^^^^  ^^^^^^
#                        catalog  schema
```

The catalog and schema tell Trino which connector and dataset to use
(e.g., `hive` for data on S3, `tpch` for built-in benchmark data).

### 2. Authentication
Trino does not use traditional username/password by default. In development
mode it accepts any username with no password. For production it uses
certificates or OAuth. For the demo, use the no-auth development setup.

### 3. SQL dialect differences

| Feature | Postgres | Trino |
|---|---|---|
| String type | `TEXT` or `VARCHAR` | `VARCHAR` only |
| Auto-increment | `SERIAL` | Not supported (Trino is read-heavy) |
| Case sensitivity | Identifiers lowercase by default | Identifiers always lowercase |
| `ILIKE` | ✅ Supported | ❌ Use `LOWER(col) LIKE LOWER(...)` |
| `LIMIT` | `LIMIT n` | `LIMIT n` ✅ (same) |
| Division | Integer division truncates | Same |

For a basic demo (SELECT, WHERE, JOIN), Trino SQL is compatible with what
`JdbcExecutor` generates. Dialect issues only arise with advanced functions.

### 4. Trino is read-only by default
Trino is designed for analytics queries — it reads from external data sources
(Hive, Iceberg, TPCH). Writing back is possible but connector-dependent. For
the demo, focus on read pipelines.

---

## Testing Strategy

### Step 1 — Local Trino with Docker

Trino provides an official Docker image. The simplest setup uses the built-in
`tpch` connector which provides standard benchmark tables (no external storage
needed):

```bash
# Start Trino with the tpch connector
docker run -d --name trino -p 8080:8080 trinodb/trino

# Wait ~30 seconds for startup, then verify
docker exec trino trino --execute "SELECT * FROM tpch.sf1.nation LIMIT 5"
```

The `tpch` catalog has tables like `nation`, `region`, `orders`, `customer`
at various scale factors (`sf1`, `sf10`, etc.) — perfect for demos.

### Step 2 — JDBC connectivity smoke test

Before writing Wayang code, verify the JDBC driver can connect:

```java
Connection conn = DriverManager.getConnection(
    "jdbc:trino://localhost:8080/tpch/sf1", "trino", null);
ResultSet rs = conn.createStatement().executeQuery(
    "SELECT name FROM nation LIMIT 3");
while (rs.next()) System.out.println(rs.getString(1));
```

### Step 3 — Unit tests (no Docker)

Follow the Postgres test pattern — mock the JDBC connection and verify SQL
generation logic. These run in CI without any external dependency.

### Step 4 — Integration tests (Docker)

Add a JUnit integration test that starts Trino via
[Testcontainers](https://testcontainers.com/) and runs a full Wayang pipeline:

```java
@Testcontainers
class TrinoIntegrationTest {

    @Container
    static TrinoContainer trino = new TrinoContainer("trinodb/trino");

    @Test
    void testFilterOnNationTable() {
        WayangContext context = new WayangContext()
            .withPlugin(Trino.plugin())
            .withPlugin(Java.basicPlugin());

        new JavaPlanBuilder(context)
            .readTable(new TableSource("nation", "nationkey", "name", "regionkey"))
            .filter(/* regionkey = 1 */)
            .collect()
            .forEach(System.out::println);
    }
}
```

Testcontainers has a first-class `TrinoContainer` class — no manual Docker
management needed in tests.

### Step 5 — Demo pipeline

Build a WordCount or analytics demo using the `tpch.sf1.orders` table:

```java
// Find top 5 regions by total order price
WayangContext context = new WayangContext()
    .withPlugin(Trino.plugin())
    .withPlugin(Java.basicPlugin());

new JavaPlanBuilder(context)
    .readTable(new TableSource("orders", "orderkey", "totalprice", "orderstatus"))
    .filter(/* orderstatus = 'F' */)   // executed in Trino as SQL WHERE
    .project("orderkey", "totalprice") // executed in Trino as SQL SELECT
    .collect()
    .forEach(System.out::println);
```

---

## Estimated Effort

| Phase | Task | Estimated time |
|---|---|---|
| 1 | Docker setup, JDBC smoke test | 1-2 days |
| 2 | Module scaffold, pom.xml, compile | 1-2 days |
| 3 | TrinoPlatform, Plugin, Mappings, Operators | 3-4 days |
| 4 | SQL dialect fixes, properties file | 1-2 days |
| 5 | Unit tests + integration tests + demo | 3-5 days |
| — | **Total** | **~2-3 weeks** |

---

## References

- [Trino JDBC Driver docs](https://trino.io/docs/current/client/jdbc.html)
- [Trino Docker image](https://hub.docker.com/r/trinodb/trino)
- [Testcontainers Trino module](https://java.testcontainers.org/modules/trino/)
- [Postgres connector source](../wayang-platforms/wayang-postgres/) — primary reference
- [JDBC template source](../wayang-platforms/wayang-jdbc-template/) — shared logic
- [Wayang architecture guide](./wayang-architecture-jdbc.md)
