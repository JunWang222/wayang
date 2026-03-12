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

# Trino Connector — Architecture & Workflow Diagrams

This document contains architecture and workflow diagrams showing how the Trino
connector classes fit into the Wayang engine. All diagrams use the Trino
connector as the example, but the same pattern applies to any JDBC-based
connector (Postgres, SQLite, etc.).

---

## Diagram 1 — How a Plugin Registers with Wayang

Shows what happens when the user calls `withPlugin(Trino.plugin())`.

```
User code                    WayangContext
─────────                    ────────────
new WayangContext()
  .withPlugin(Trino.plugin())
        │
        ▼
  TrinoPlugin
  ├── getRequiredPlatforms() ──► registers TrinoPlatform singleton
  ├── getMappings()          ──► registers [FilterMapping, ProjectionMapping, ...]
  └── getChannelConversions()──► registers [SqlQueryChannel → StreamChannel, ...]

  (all stored inside WayangContext, used later by the optimizer)
```

---

## Diagram 2 — How the Optimizer Picks Trino

Shows how the optimizer uses `FilterMapping` to decide whether to run a logical
operator on Trino.

```
User writes logical plan:
  TableSource ──► FilterOperator ──► ProjectionOperator

                    Optimizer
                    ─────────
  For each registered Mapping:

    FilterMapping.getTransformations()
        │
        ├── SubplanPattern: "find a FilterOperator<Record>
        │                    with a SQL implementation"
        │
        └── does it match? ──YES──► replace with TrinoFilterOperator
                                    (tagged for TrinoPlatform)
                           ──NO───► leave as logical operator
                                    (Java Streams handles it instead)

  After all mappings applied, optimizer has candidate physical plans:
    Plan A: all on Trino             cost = X
    Plan B: all on Java Streams      cost = Y
    Plan C: Trino filter + Java map  cost = Z

  Picks lowest cost → produces ExecutionPlan
```

---

## Diagram 3 — Class Relationships

Shows how all the connector classes relate to each other and to Wayang core.

```
              Plugin (interface)
                    │
              TrinoPlugin
              ├── requires: TrinoPlatform, JavaPlatform
              ├── mappings: FilterMapping, ProjectionMapping
              └── conversions: SqlQueryChannel → StreamChannel


              Platform (abstract)
                    │
        JdbcPlatformTemplate (abstract)
         ├── getExecutorFactory() → JdbcExecutor
         ├── createDatabaseDescriptor()
         ├── createLoadProfileToTimeConverter()
         └── createTimeToCostConverter()
                    │
              TrinoPlatform
              └── getJdbcDriverClassName() → "io.trino.jdbc.TrinoDriver"


              Mapping (interface)
                    │
              FilterMapping
              └── getTransformations()
                    └── PlanTransformation
                         ├── SubplanPattern  (matches FilterOperator<Record>)
                         ├── ReplacementSubplanFactory
                         │    └── creates TrinoFilterOperator
                         └── target: TrinoPlatform


              JdbcExecutionOperator (interface)
              TrinoExecutionOperator (marker interface)
                    │
             ┌──────┴──────────────┐
    TrinoTableSource  TrinoFilterOperator  TrinoProjectionOperator
         │                  │                      │
    JdbcTableSource   JdbcFilterOperator   JdbcProjectionOperator
         │                  │                      │
         └──────────────────┴──────────────────────┘
                            │
                    createSqlClause()  ← each builds its own SQL fragment
```

---

## Diagram 4 — End-to-End Execution Flow

Traces a full pipeline from `.collect()` to SQL results coming back from Trino.

```
.collect() called
      │
      ▼
CrossPlatformExecutor
      │
      ├── gets ExecutionPlan from optimizer
      │
      └── for each ExecutionStage (grouped by platform):

            Stage: [TrinoPlatform]
            Tasks: TableSource → FilterOperator → ProjectionOperator
                  │
                  ▼
            TrinoPlatform.getExecutorFactory()
                  │ returns
                  ▼
            JdbcExecutor (inherited — no new code needed)
            JdbcExecutor.execute(stage)
                  │
                  ├── walks task chain:
                  │     startTask  = TrinoTableSource      → "nation"
                  │     nextTask   = TrinoFilterOperator   → "name LIKE 'A%'"
                  │     nextTask   = TrinoProjectionOperator → "nationkey, name"
                  │
                  ├── createSqlString():
                  │     SELECT nationkey, name
                  │     FROM nation
                  │     WHERE name LIKE 'A%';
                  │
                  ├── sends SQL via JDBC → Trino server
                  │                         │
                  │                         └── Trino executes on
                  │                             Hive/Iceberg/TPCH data
                  │
                  └── ResultSet → Iterable<Record> → returned to user
```

---

## Diagram 5 — What's New vs. What's Inherited

Shows exactly how much of the work is already done by `wayang-jdbc-template`
vs. what the new Trino module needs to provide.

```
┌─────────────────────────────────────────────────────────────┐
│                   wayang-jdbc-template                      │
│                   (EXISTS, NOT TOUCHED)                     │
│                                                             │
│  JdbcPlatformTemplate   JdbcExecutor   SqlQueryChannel      │
│  JdbcTableSource        JdbcFilterOp   JdbcProjectionOp     │
│                                                             │
│  ← ALL SQL generation, connection management, execution →   │
└─────────────────────────────────────────────────────────────┘
              ▲ extends / uses
┌─────────────────────────────────────────────────────────────┐
│                   wayang-trino (NEW)                        │
│                                                             │
│  TrinoPlatform      ← 1 method: getJdbcDriverClassName()   │
│  TrinoPlugin        ← wiring: platforms + mappings          │
│  FilterMapping      ← pattern match + replacement factory   │
│  TrinoFilterOperator← empty marker class                    │
│  TrinoTableSource   ← empty marker class                    │
│  defaults.properties← JDBC URL + cost config values        │
└─────────────────────────────────────────────────────────────┘
```

**Key insight:** the JDBC template is the engine; the Trino module is just the
configuration and registration layer on top of it. ~80% of the implementation
is inherited for free.
