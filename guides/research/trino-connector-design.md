# Trino Connector Design

## Overview

This document describes the design and implementation of the Trino connector
for Apache Wayang. The connector allows Wayang to push SQL operators
(TableScan, Filter, Projection) down to a Trino cluster and retrieve results
as a Java stream for further processing.

---

## High-Level Approach

### Key Decision: Extend `wayang-generic-jdbc`, not a new module

Two integration strategies were considered.

**Option A — New standalone module** (`wayang-trino`)  
Mirror the Postgres connector: create a new Maven module with its own
executor, operators, mappings, plugin, and channel conversions.

**Option B — Subclass `wayang-generic-jdbc`** (chosen)  
Create Trino-specific classes *inside* the existing `wayang-generic-jdbc`
module that extend the generic JDBC base classes.

Option B was chosen for the following reasons:

| Concern | Option A | Option B |
|---|---|---|
| Code volume | Large — duplicate of Postgres structure | Small — ~7 files, each ≤ 50 lines |
| SQL generation reuse | Must reimplement | Inherits from `GenericJdbcExecutor` |
| Connection management reuse | Must reimplement | Inherits `createDatabaseDescriptor()` |
| Independent cost model | Yes | Yes (with one constructor addition) |
| Separate namespace | Yes | Yes — `CONFIG_NAME = "trino"` |
| New Maven module needed | Yes | No |
| Future Trino SQL dialect overrides | Separate executor | Override methods in `TrinoExecutor` |

The critical enabler for Option B is that `JdbcPlatformTemplate` derives
**all** property key prefixes from `getPlatformId()`, which returns the
`CONFIG_NAME` set at construction. Setting `CONFIG_NAME = "trino"` makes
the platform automatically read `wayang.trino.*` for connection, cost, and
hardware properties — identical in effect to a standalone module.

---

## Architecture

### Before: `wayang-generic-jdbc` with a runtime string

Before this change, a user had to pass the engine name as a constructor
argument every time they declared a source:

```java
new GenericJdbcTableSource("trino", "iceberg.sales.orders", ...)
```

All engines shared the same cost model keys (`wayang.genericjdbc.*`), so
the Wayang optimizer could not differentiate the cost of running a query
on Trino versus any other engine registered through `GenericJdbc`.

### After: Typed platform subclass per engine

```
JdbcPlatformTemplate              (wayang-jdbc-template — unchanged)
├── PostgresPlatform              CONFIG_NAME = "postgres"   ← existing
└── GenericJdbcPlatform           CONFIG_NAME = "genericjdbc"
    └── TrinoPlatform             CONFIG_NAME = "trino"      ← NEW
```

Each platform subclass sets its own `CONFIG_NAME`. That single string
propagates through the entire Wayang property system:

```
CONFIG_NAME = "trino"
    │
    ├── Connection:   wayang.trino.jdbc.url
    │                 wayang.trino.jdbc.user
    │                 wayang.trino.jdbc.password
    │                 wayang.trino.jdbc.driverName
    │
    ├── Hardware:     wayang.trino.cpu.mhz
    │                 wayang.trino.cores
    │                 wayang.trino.costs.fix
    │                 wayang.trino.costs.per-ms
    │
    └── Cost model:   wayang.trino.tablesource.load
                      wayang.trino.filter.load
                      wayang.trino.projection.load
                      wayang.trino.sqltostream.load.query
                      wayang.trino.sqltostream.load.output
```

---

## How Trino Integrates with `GenericJdbc`

### Class structure

```
GenericJdbcPlatform
└── TrinoPlatform               CONFIG_NAME = "trino"
                                getExecutorFactory() → TrinoExecutor

GenericJdbcExecutor
└── TrinoExecutor               empty today; extension point for SQL dialect

GenericJdbcTableSource
└── TrinoTableSource            overrides getPlatform() → TrinoPlatform
                                jdbcName fixed to "trino"

GenericJdbcFilterOperator
└── TrinoFilterOperator         overrides getPlatform() → TrinoPlatform

GenericJdbcProjectionOperator
└── TrinoProjectionOperator     overrides getPlatform() → TrinoPlatform
```

### Why `getPlatform()` is the central override

`JdbcExecutionOperator` defines two default methods that key off `getPlatform()`:

```java
// Which SQL channel does this operator produce/consume?
default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
    return Collections.singletonList(this.getPlatform().getSqlQueryChannelDescriptor());
}

// Which cost-model key to read for load estimation?
// (in JdbcTableSource)
public String getLoadProfileEstimatorConfigurationKey() {
    return String.format("wayang.%s.tablesource.load", this.getPlatform().getPlatformId());
}
```

By overriding `getPlatform()` in `TrinoTableSource` (and friends) to return
`TrinoPlatform.getInstance()`, the output channel is tied to the Trino
platform and the cost key prefix becomes `"trino"` — without touching
any framework code.

### Channel descriptor equality

`SqlQueryChannel.Descriptor.equals()` compares the wrapped platform by
object identity (via `Objects.equals`). Because `TrinoPlatform` is a
singleton, all `Descriptor` objects wrapping it compare as equal. This means
the optimizer correctly matches a `TrinoTableSource` output channel to the
`TrinoChannelConversions.SQL_TO_STREAM_CONVERSION` input channel without any
special registration.

### Execution flow end-to-end

```
1. User declares plan
   TrinoTableSource("iceberg.sales.orders") → FilterOperator → sink

2. Optimizer — planning phase
   TrinoFilterMapping maps FilterOperator → TrinoFilterOperator
   (targeting TrinoPlatform)

3. Optimizer — cost estimation
   TrinoTableSource.getCardinalityEstimator():
     reads wayang.trino.jdbc.* → opens JDBC → runs SELECT count(*)

   TrinoTableSource.getLoadProfileEstimatorConfigurationKey():
     → "wayang.trino.tablesource.load"  (α=10, β=800k)

   Compares against Java/Spark cost to decide whether to push down to Trino.

4. Execution — SQL assembly (TrinoExecutor)
   Inherits GenericJdbcExecutor.execute():
     SELECT {projection} FROM iceberg.sales.orders WHERE {conditions}
   Stores the SQL string in a SqlQueryChannel (no DB call yet).

5. Execution — materialization (GenericSqlToStreamOperator)
   Wrapped around TrinoPlatform, so:
     reads wayang.trino.jdbc.* → opens JDBC connection
     runs the assembled SQL
     streams ResultSet as Java Stream<Record>

6. Results flow to downstream Java operators or sink.
```

### SQL compatibility

Trino speaks standard ANSI SQL for the operators Wayang currently pushes
down. The only dialect fix required was removing trailing semicolons from
generated queries (done in `GenericJdbcExecutor` and
`GenericJdbcTableSource`). No Trino-specific SQL generation is needed today.

`TrinoExecutor` exists as a named extension point. Future Trino-specific SQL
features (e.g. `TABLESAMPLE`, connector hints, `LIMIT`/`OFFSET`) can be
added by overriding `createSqlQuery()` or adding helper methods there.

---

## How the Cost Model Integrates with Trino

### The problem with `GenericJdbc` before

`GenericSqlToStreamOperator` previously hardcoded the platform ID:

```java
// Before — always reads wayang.genericjdbc.sqltostream.load.query
String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId())
//                                                 ↑ always "genericjdbc"
```

Even if you set `wayang.trino.sqltostream.load.query` in your config, it
was never read. Every engine registered through `GenericJdbc` shared the
same α/β values.

### The fix

Because `GenericSqlToStreamOperator` stores a reference to the
`jdbcPlatform` it wraps, and because `TrinoChannelConversions` creates it
with `TrinoPlatform.getInstance()`:

```java
new GenericSqlToStreamOperator(TrinoPlatform.getInstance())
//                              ↑ jdbcPlatform field
```

…the same `getPlatformId()` call now returns `"trino"`:

```java
// After — reads wayang.trino.sqltostream.load.query
String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId())
//                                                 ↑ now "trino"
```

No change to `GenericSqlToStreamOperator` was needed. The fix was
structural: using a typed `TrinoPlatform` instance instead of the generic
one.

### Cost model values

Cost formulas follow the pattern `cpu = α * rows + β`, parameterised for
each engine's performance profile.

| Parameter | Postgres | GenericJdbc | **Trino** |
|---|---|---|---|
| α (cpu per output row) | 55 | 55 | **10** |
| β (fixed startup cost) | 380 000 | 380 000 | **800 000** |

**Rationale for Trino values:**

- **α = 10** — Trino executes scans in parallel across workers. Per-row CPU
  cost visible to the coordinator is ~5× lower than a single-node JDBC
  source, which must fetch each row serially.
- **β = 800 000** — Trino has higher fixed overhead: query parsing,
  distributed planning, coordinator–worker handshake, and result
  serialisation. A query against even a tiny table incurs this cost.

**Optimizer implication** — the crossover point where Trino becomes cheaper
than Postgres (at equal α, β):

```
Trino wins when:  10*n + 800000 < 55*n + 380000
                  420000 < 45*n
                  n > ~9300 rows
```

So for tables with fewer than ~10k rows the optimizer will prefer Postgres
or Java; for larger tables it will route to Trino. These thresholds can be
tuned after real benchmarks by updating `wayang-trino-defaults.properties`.

### Defaults file

`wayang-trino-defaults.properties` is loaded automatically when
`TrinoPlatform.configureDefaults()` is called (triggered by
`WayangContext.withPlugin(Trino.plugin())`). It provides:

- Hardware profile (`cpu.mhz`, `cores`, `costs.*`)
- Default JDBC driver class (`io.trino.jdbc.TrinoDriver`)
- All five cost-model keys with template and concrete values

Connection URL and credentials are intentionally not in the defaults file —
they are deployment-specific and must be set in the application's
`wayang.properties` or `Configuration` object.

---

## Implementation Details

### Files added or modified

#### New files

| File | Lines | Role |
|---|---|---|
| `platform/TrinoPlatform.java` | 60 | Platform singleton, `CONFIG_NAME = "trino"` |
| `execution/TrinoExecutor.java` | 35 | Executor (delegates to `GenericJdbcExecutor`) |
| `operators/TrinoTableSource.java` | 55 | Source operator; overrides `getPlatform()` |
| `operators/TrinoFilterOperator.java` | 50 | Filter pushdown; overrides `getPlatform()` |
| `operators/TrinoProjectionOperator.java` | 55 | Projection pushdown; overrides `getPlatform()` |
| `channels/TrinoChannelConversions.java` | 45 | SQL→Stream conversion wrapping `TrinoPlatform` |
| `mapping/TrinoFilterMapping.java` | 60 | Registers Filter→SQL mapping for `TrinoPlatform` |
| `mapping/TrinoProjectionMapping.java` | 65 | Registers Projection→SQL mapping for `TrinoPlatform` |
| `plugin/TrinoPlugin.java` | 65 | Bundles platform + mappings + conversions |
| `Trino.java` | 40 | Public entry point: `Trino.plugin()`, `Trino.platform()` |
| `resources/wayang-trino-defaults.properties` | 90 | Cost model defaults |

#### Modified files

| File | Change |
|---|---|
| `platform/GenericJdbcPlatform.java` | Added `protected GenericJdbcPlatform(String, String)` constructor so subclasses can set their own `CONFIG_NAME` |
| `test/TrinoGenericJdbcIT.java` | Switched from `GenericJdbcTableSource("trino", …)` to `TrinoTableSource(…)` and from `GenericJdbc.plugin()` to `Trino.plugin()` |

### User-facing API change

```java
// Before
new WayangContext(config)
    .withPlugin(Java.basicPlugin())
    .withPlugin(GenericJdbc.plugin());           // generic plugin

new GenericJdbcTableSource("trino", "iceberg.sales.orders", "col1", ...)
//                          ↑ magic string, no IDE help

// After
new WayangContext(config)
    .withPlugin(Java.basicPlugin())
    .withPlugin(Trino.plugin());                 // typed, self-documenting

new TrinoTableSource("iceberg.sales.orders", "col1", ...)
//  ↑ typed class, no magic string
```

### Configuration required per deployment

```properties
# wayang.properties (or set programmatically via Configuration)
wayang.trino.jdbc.url      = jdbc:trino://trino-host:8080
wayang.trino.jdbc.user     = admin
wayang.trino.jdbc.password =
```

Everything else (`driverName`, cost model, hardware) is supplied by
`wayang-trino-defaults.properties` and does not need to be set by the user.

### Integration test coverage

Six tests in `TrinoGenericJdbcIT` cover the end-to-end pipeline against a
real local Trino instance (Docker + Iceberg + MinIO + Hive Metastore):

| Test | What it verifies |
|---|---|
| `testTableScan` | Full scan returns all 10 rows |
| `testFilterPushdown` | String filter pushed to SQL; only matching rows returned |
| `testFilterNumeric` | Numeric filter pushed to SQL |
| `testProjectionPushdown` | Column pruning pushed to SQL; only requested fields returned |
| `testFilterThenProjection` | Combined filter + projection pipeline |
| `testCardinalityMatches` | Exact row count matches expected value (validates cardinality estimator) |

Run with:
```bash
cd trino-setup && docker-compose up -d && ./scripts/run-init.sh
mvn test -pl wayang-platforms/wayang-generic-jdbc \
    -Dtest=TrinoGenericJdbcIT -Pintegration,skip-prerequisite-check \
    -Drat.skip=true -Dmaven.javadoc.skip=true
```

---

## Extension Points

| Scenario | Where to add code |
|---|---|
| Trino-specific SQL syntax (TABLESAMPLE, hints) | Override `createSqlQuery()` in `TrinoExecutor` |
| Trino-specific identifier quoting | Add `quoteIdentifier()` hook to `GenericJdbcExecutor`; override in `TrinoExecutor` |
| Measured α/β values after benchmarking | Update `wayang-trino-defaults.properties` |
| Trino JOIN pushdown | Add `TrinoJoinOperator` + `TrinoJoinMapping` (mirrors Postgres join pattern) |
| Multi-tenant Trino (different clusters) | Subclass `TrinoPlatform` with a different `CONFIG_NAME` (e.g. `"trino-prod"`) |
