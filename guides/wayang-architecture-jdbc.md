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

# Wayang Architecture & the JDBC Connector

This guide explains how Apache Wayang is structured and traces exactly how the
Postgres connector works end-to-end, from a user API call down to a SQL query
being sent to the database.

---

## 1. Layered Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                   User / API Layer                  │
│   PlanBuilder  ·  DataQuanta  ·  SQL API            │
└───────────────────────┬─────────────────────────────┘
                        │ builds
                        ▼
┌─────────────────────────────────────────────────────┐
│               Logical Plan (WayangPlan)             │
│  FilterOperator · ProjectionOperator · TableSource  │
│        (platform-independent, abstract)             │
└───────────────────────┬─────────────────────────────┘
                        │ fed into
                        ▼
┌─────────────────────────────────────────────────────┐
│                   Optimizer                         │
│  - Enumerates possible physical plans               │
│  - Applies Mappings (logical → physical operators)  │
│  - Estimates cost via LoadProfileToTimeConverter    │
│  - Selects cheapest ExecutionPlan                   │
└───────────────────────┬─────────────────────────────┘
                        │ produces
                        ▼
┌─────────────────────────────────────────────────────┐
│              Physical Execution Plan                │
│  ExecutionStage (per platform) containing           │
│  ExecutionTasks linked by Channels                  │
└───────────────────────┬─────────────────────────────┘
                        │ dispatched to
                        ▼
┌─────────────────────────────────────────────────────┐
│              Platform  /  Executor                  │
│  PostgresPlatform → JdbcExecutor                    │
│  SparkPlatform    → SparkExecutor                   │
│  JavaPlatform     → JavaExecutor                    │
└─────────────────────────────────────────────────────┘
```

The key insight: **the user writes a platform-independent plan; the optimizer
decides which engine(s) to use.**

---

## 2. How the Postgres Connector Fits In

The Postgres connector lives in `wayang-platforms/wayang-postgres` and is built
on top of the shared JDBC template in `wayang-platforms/wayang-jdbc-template`.

```
wayang-platforms/
├── wayang-jdbc-template/       ← shared JDBC logic (executor, SQL builder, channels)
│   └── JdbcPlatformTemplate
│   └── JdbcExecutor
│   └── JdbcTableSource / JdbcFilterOperator / JdbcProjectionOperator
│   └── SqlQueryChannel
│
└── wayang-postgres/            ← Postgres-specific thin layer
    ├── PostgresPlatform        ← registers JDBC driver class name
    ├── PostgresPlugin          ← registers mappings + channel conversions
    ├── mapping/
    │   ├── FilterMapping       ← logical FilterOperator → PostgresFilterOperator
    │   ├── ProjectionMapping   ← logical ProjectionOperator → PostgresProjectionOperator
    │   └── JoinMapping
    └── operators/
        ├── PostgresTableSource
        ├── PostgresFilterOperator
        └── PostgresProjectionOperator
```

---

## 3. Core Classes and Their Responsibilities

| Class | Module | Responsibility |
|---|---|---|
| `Platform` | `wayang-core` | Abstract base for every execution engine. Provides `getExecutorFactory()`, cost converters, and configuration. |
| `Plugin` | `wayang-core` | Registers a platform's mappings, channel conversions, and config properties into a `WayangContext`. |
| `Mapping` | `wayang-core` | Describes how one logical operator pattern is replaced with a physical (platform-specific) operator. Contains a `SubplanPattern` and a `ReplacementSubplanFactory`. |
| `Executor` | `wayang-core` | Executes one `ExecutionStage` on a specific platform. Created fresh per `Job`. |
| `SqlQueryChannel` | `wayang-jdbc-template` | The channel type that carries a SQL query string between JDBC execution tasks instead of actual data. The query is materialized only at the final stage boundary. |
| `JdbcPlatformTemplate` | `wayang-jdbc-template` | Abstract base for all JDBC platforms. Provides `createDatabaseDescriptor()`, `getExecutorFactory()` (always returns `JdbcExecutor`), and cost converter wiring. Subclasses only need to provide `getJdbcDriverClassName()`. |
| `JdbcExecutor` | `wayang-jdbc-template` | The actual JDBC executor. Traverses an `ExecutionStage`, assembles the operator chain into a single SQL string, executes it, and registers the result channel. |
| `JdbcTableSource` / `JdbcFilterOperator` / `JdbcProjectionOperator` | `wayang-jdbc-template` | Physical operators that know how to render themselves as SQL clauses (`createSqlClause()`). |
| `PostgresPlatform` | `wayang-postgres` | Concrete `JdbcPlatformTemplate`. Only overrides `getJdbcDriverClassName()` → `org.postgresql.Driver`. |
| `PostgresFilterOperator` | `wayang-postgres` | Thin subclass of `JdbcFilterOperator`. Marker for the Postgres platform; no SQL logic change needed. |
| `FilterMapping` | `wayang-postgres` | Tells the optimizer: "if you see a logical `FilterOperator<Record>` that has a SQL implementation, replace it with a `PostgresFilterOperator`." |

---

## 4. End-to-End Sequence: User Query → SQL Execution

The following diagram traces a simple query:

```java
WayangContext context = new WayangContext().withPlugin(Postgres.plugin());
JavaPlanBuilder plan = new JavaPlanBuilder(context);

plan.readTable(new TableSource("employees"))
    .filter(/* age > 30 */)
    .project(/* name, salary */)
    .collect();
```

```
User Code                Wayang Core              Postgres Connector
─────────                ───────────              ──────────────────

withPlugin(Postgres.plugin())
    │
    └──► PostgresPlugin.getMappings()
              returns [FilterMapping, ProjectionMapping, ...]
              registered into WayangContext

.readTable().filter().project().collect()
    │
    └──► PlanBuilder builds WayangPlan
              TableSource (logical)
                  └─► FilterOperator (logical)
                          └─► ProjectionOperator (logical)

         Optimizer.optimize(WayangPlan)
              │
              ├── Applies FilterMapping
              │       matches logical FilterOperator
              │       replaces with PostgresFilterOperator
              │
              ├── Applies ProjectionMapping
              │       replaces with PostgresProjectionOperator
              │
              ├── Estimates cost
              │       PostgresPlatform.createLoadProfileToTimeConverter()
              │       PostgresPlatform.createTimeToCostConverter()
              │
              └── Selects cheapest plan → all on Postgres
                      produces ExecutionPlan
                          └── ExecutionStage (Postgres)
                                  ├── PostgresTableSource task
                                  ├── PostgresFilterOperator task
                                  └── PostgresProjectionOperator task

         CrossPlatformExecutor dispatches stage
              │
              └──► PostgresPlatform.getExecutorFactory()
                        returns new JdbcExecutor(platform, job)

                   JdbcExecutor.execute(stage)
                        │
                        ├── Walks ExecutionStage task chain
                        │       startTask  → PostgresTableSource
                        │       nextTask   → PostgresFilterOperator
                        │       nextTask   → PostgresProjectionOperator
                        │
                        ├── createSqlString()
                        │       tableOp.createSqlClause()   → "employees"
                        │       filterOp.createSqlClause()  → "age > 30"
                        │       projOp.createSqlClause()    → "name, salary"
                        │
                        │   Assembles:
                        │       SELECT name, salary
                        │       FROM employees
                        │       WHERE age > 30;
                        │
                        ├── Executes query via JDBC Connection
                        │
                        └── Registers SqlQueryChannel with result
                                 → returned to user as Iterable<Record>
```

---

## 5. Key Design Decisions

**SQL is built lazily, not eagerly.**
`JdbcExecutor.execute()` does not run the SQL immediately. It assembles the
query string and stores it in a `SqlQueryChannel`. The query is only executed
when the result is consumed at a stage boundary (e.g., when another platform
reads the data, or `.collect()` is called).

**The JDBC template does almost all the work.**
`PostgresPlatform` is ~15 lines of code. `PostgresFilterOperator` is ~20 lines.
All SQL assembly, connection management, and execution logic lives in
`wayang-jdbc-template` and is reused by every JDBC-based platform.

**Mappings gate on SQL availability.**
`FilterMapping` checks `op.getPredicateDescriptor().getSqlImplementation() != null`
before allowing the replacement. This means if a filter predicate has no SQL
form (e.g., a complex Java lambda), it won't be pushed down to Postgres —
Wayang will fall back to processing it in Java Streams instead.
