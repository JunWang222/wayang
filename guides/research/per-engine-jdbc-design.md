# Per-Engine JDBC Connector Design: SQL Compatibility & Cost Models

## 1. Current Architecture

### Class hierarchy (as-is)

```
JdbcPlatformTemplate          (wayang-jdbc — abstract base)
├── PostgresPlatform          CONFIG_NAME = "postgres"
└── GenericJdbcPlatform       CONFIG_NAME = "genericjdbc"
```

`PostgresPlatform` and `GenericJdbcPlatform` both extend the same abstract base.
Postgres has its **own operator classes, plugin, and mappings**.
`GenericJdbc` is a single shared adapter that accepts a runtime `jdbcName` string
(`"trino"`, `"bigquery"`, …) to vary the JDBC **connection** properties.

### How config keys are constructed today

| What is read | Key pattern | Populated from |
|---|---|---|
| JDBC connection URL | `wayang.{jdbcName}.jdbc.url` | `jdbcName` arg at construction |
| JDBC driver class | `wayang.{jdbcName}.jdbc.driverName` | same |
| **Cost model** | `wayang.genericjdbc.*.load` | `CONFIG_NAME` — hardcoded `"genericjdbc"` |

The connection keys are engine-specific. The cost keys are **not**; every engine
registered under `GenericJdbc` shares the same `wayang.genericjdbc.*` cost model.

### Where config is read in code

```
GenericJdbcPlatform.createDatabaseDescriptor(config, jdbcName)
  └─ reads  wayang.{jdbcName}.jdbc.url / .user / .password / .driverName

GenericSqlToStreamOperator.evaluate()   (line 106)
  └─ reads  wayang.genericjdbc.sqltostream.load.query   ← CONFIG_NAME hardcoded

GenericJdbcTableSource.getCardinalityEstimator()
  └─ reads  wayang.{jdbcName}.jdbc.*   to open connection for COUNT(*)
```

---

## 2. Problem Statement

### 2a. SQL Compatibility

`GenericJdbcExecutor.createSqlQuery()` emits standard ANSI SQL:

```sql
SELECT {projection} FROM {table} WHERE {conditions}
```

Both Trino and BigQuery are mostly ANSI-compatible, but each has dialect quirks
that will cause parse errors or incorrect results.

#### Trino quirks

| Issue | Example | Status |
|---|---|---|
| No trailing semicolon | `SELECT 1;` → parse error | **Fixed** (semicolon removed) |
| 3-part table names | `catalog.schema.table` | Works — user passes it as tableName |
| `LIMIT` / `OFFSET` | Standard ANSI `LIMIT n` | Works |
| String literals | Single-quoted `'value'` | Works |
| Identifier quoting | Double-quotes `"col"` | Works |
| `CAST` syntax | `CAST(x AS VARCHAR)` | Works |
| No `ILIKE` | use `LOWER(col) LIKE LOWER(?)` | Not yet needed |

**Summary**: Trino is already compatible with the current SQL generation after the semicolon fix.

#### BigQuery quirks

| Issue | Example | Status |
|---|---|---|
| No trailing semicolon | Causes parse error | **Fixed** |
| Table name quoting | `` `project.dataset.table` `` | Works — user passes backticks in tableName |
| Column name quoting | `` `col` `` (backticks, not double-quotes) | **Not handled** |
| `LIMIT` | Standard `LIMIT n` | Works |
| No `!=` | Use `<>` instead | Not yet needed |
| `CURRENT_DATE` | No parentheses (vs `CURRENT_DATE()`) | Not yet needed |
| `DATE` literals | `DATE '2021-01-01'` vs `'2021-01-01'` | Not yet needed |

**Summary**: BigQuery needs backtick-quoted identifiers when column names are
passed explicitly in a `SELECT` projection clause. Today the projection comes
from `GenericJdbcProjectionOperator.createSqlClause()` which uses raw field
names (e.g., `region, amount`) — no quoting. This is fine unless a column name
is a reserved word.

### 2b. Cost Model

All engines share `wayang.genericjdbc.sqltostream.load.query` etc.
The Wayang optimizer cannot distinguish Trino's cost from BigQuery's cost,
so it may route a query to the wrong platform.

---

## 3. Proposed Solution: Inheritance (mirrors Postgres pattern)

### Approach

Create a **concrete Platform subclass per engine**, each with its own
`CONFIG_NAME`. This is identical to how `PostgresPlatform` works relative
to `JdbcPlatformTemplate`.

```
JdbcPlatformTemplate          (wayang-jdbc — unchanged)
├── PostgresPlatform          CONFIG_NAME = "postgres"
├── GenericJdbcPlatform       CONFIG_NAME = "genericjdbc"   ← keep as fallback
│   ├── TrinoPlatform         CONFIG_NAME = "trino"          ← NEW
│   └── BigQueryPlatform      CONFIG_NAME = "bigquery"       ← NEW
```

Because `TrinoPlatform.getPlatformId()` returns `"trino"`, the existing line
in `GenericSqlToStreamOperator`:

```java
String.format("wayang.%s.sqltostream.load.query", this.jdbcPlatform.getPlatformId())
```

**already produces the right key** (`wayang.trino.sqltostream.load.query`)
without any change to that operator. The cost and connection keys now both use
the same `CONFIG_NAME` — exactly how Postgres works.

### What needs to be created per engine

For each new engine (shown here for Trino; BigQuery is identical structure):

#### a. Platform class

```java
// wayang-platforms/wayang-generic-jdbc/src/main/java/
//   org/apache/wayang/genericjdbc/platform/TrinoPlatform.java

public class TrinoPlatform extends GenericJdbcPlatform {

    private static final String PLATFORM_NAME = "Trino";
    private static final String CONFIG_NAME    = "trino";

    private static TrinoPlatform instance;

    public static TrinoPlatform getInstance() {
        if (instance == null) instance = new TrinoPlatform();
        return instance;
    }

    protected TrinoPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new TrinoExecutor(this, job);   // dialect-aware executor
    }

    @Override
    public String getJdbcDriverClassName() {
        return "io.trino.jdbc.TrinoDriver";
    }
}
```

#### b. Executor (SQL dialect)

```java
// TrinoExecutor.java
public class TrinoExecutor extends GenericJdbcExecutor {

    public TrinoExecutor(TrinoPlatform platform, Job job) {
        super(platform, job);
    }

    // Trino-specific overrides go here.
    // Currently none needed beyond the semicolon fix already in base class.
    // Example: override quoteIdentifier() if needed in future.
}
```

For BigQuery, override `createSqlQuery()` to backtick-quote column names in
the SELECT clause when projection is explicit:

```java
// BigQueryExecutor.java
public class BigQueryExecutor extends GenericJdbcExecutor {

    @Override
    protected String quoteIdentifier(String name) {
        return "`" + name + "`";   // BigQuery uses backticks
    }
}
```

This requires extracting a `quoteIdentifier()` hook in `GenericJdbcExecutor`
(one small refactor in the base class).

#### c. TableSource

```java
// TrinoTableSource.java
public class TrinoTableSource extends GenericJdbcTableSource {
    public TrinoTableSource(String tableName, String... columnNames) {
        super("trino", tableName, columnNames);   // jdbcName fixed to "trino"
    }
}
```

User code becomes cleaner — no magic string argument:

```java
// Before
new GenericJdbcTableSource("trino", "iceberg.sales.orders", "order_id", ...)

// After
new TrinoTableSource("iceberg.sales.orders", "order_id", ...)
```

#### d. Properties file

```properties
# wayang-trino-defaults.properties  (on the classpath)

# Connection (overridden per deployment in wayang.properties)
# wayang.trino.jdbc.url      = jdbc:trino://localhost:8080
# wayang.trino.jdbc.driverName = io.trino.jdbc.TrinoDriver

# Hardware profile for cost estimation (tune after benchmarking)
wayang.trino.cpu.mhz    = 2700
wayang.trino.cores      = 4
wayang.trino.costs.fix  = 0.0
wayang.trino.costs.per-ms = 1.0

# Cost formulas  α * rows + β
# Trino is a distributed query engine: lower per-row CPU overhead
# than a single-node JDBC source, but higher fixed startup cost.
wayang.trino.tablesource.load = {\
  "in":0, "out":1,\
  "cpu":"${10*out0 + 800000}",\
  "ram":"0", "p":0.9\
}

wayang.trino.filter.load = {\
  "in":1, "out":1,\
  "cpu":"${10*in0 + 800000}",\
  "ram":"0", "p":0.9\
}

wayang.trino.projection.load = {\
  "in":1, "out":1,\
  "cpu":"${10*in0 + 800000}",\
  "ram":"0", "p":0.9\
}

wayang.trino.sqltostream.load.query = {\
  "in":1, "out":1,\
  "cpu":"${10*out0 + 800000}",\
  "ram":"0", "p":0.9\
}

wayang.trino.sqltostream.load.output = {\
  "in":1, "out":1,\
  "cpu":"${10*out0}",\
  "ram":"0", "p":0.9\
}
```

```properties
# wayang-bigquery-defaults.properties

wayang.bigquery.cpu.mhz   = 2700
wayang.bigquery.cores     = 8
wayang.bigquery.costs.fix = 0.0
wayang.bigquery.costs.per-ms = 1.0

# BigQuery is serverless and massively parallel.
# Very low per-row CPU cost; high fixed cost (query dispatch + billing).
wayang.bigquery.tablesource.load = {\
  "in":0, "out":1,\
  "cpu":"${5*out0 + 2000000}",\
  "ram":"0", "p":0.9\
}

wayang.bigquery.filter.load = {\
  "in":1, "out":1,\
  "cpu":"${5*in0 + 2000000}",\
  "ram":"0", "p":0.9\
}

wayang.bigquery.projection.load = {\
  "in":1, "out":1,\
  "cpu":"${5*in0 + 2000000}",\
  "ram":"0", "p":0.9\
}

wayang.bigquery.sqltostream.load.query = {\
  "in":1, "out":1,\
  "cpu":"${5*out0 + 2000000}",\
  "ram":"0", "p":0.9\
}

wayang.bigquery.sqltostream.load.output = {\
  "in":1, "out":1,\
  "cpu":"${5*out0}",\
  "ram":"0", "p":0.9\
}
```

#### e. Plugin

```java
// TrinoPlugin.java  (mirrors GenericJdbcPlugin exactly, uses TrinoPlatform)
public class TrinoPlugin implements Plugin {
    @Override public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(TrinoPlatform.getInstance(), JavaPlatform.getInstance());
    }
    @Override public Collection<Mapping> getMappings() {
        return Mappings.ALL;           // same mappings as GenericJdbc
    }
    @Override public Collection<ChannelConversion> getChannelConversions() {
        return GenericChannelConversions.ALL;
    }
    @Override public void setProperties(Configuration configuration) {}
}
```

---

## 4. Required Code Changes Summary

### Changes to existing files

| File | Change |
|---|---|
| `GenericJdbcPlatform.java` | Make constructor `protected` with `(String name, String configName)` params so subclasses can call `super(name, configName)` — **already possible today** |
| `GenericJdbcExecutor.java` | Extract `quoteIdentifier(String name)` hook (returns identity by default) — **1 method, ~5 lines** |
| `GenericJdbc.java` | Add `public static Plugin trinoPlugin()` and `bigqueryPlugin()` factory methods |

### New files per engine (Trino shown; BigQuery mirrors it)

```
wayang-platforms/wayang-generic-jdbc/src/main/java/…/
  platform/TrinoPlatform.java          ~30 lines
  execution/TrinoExecutor.java         ~20 lines (empty overrides for now)
  operators/TrinoTableSource.java      ~25 lines

wayang-platforms/wayang-generic-jdbc/src/main/resources/
  wayang-trino-defaults.properties     ~40 lines
```

No new Maven module is needed — all files live inside `wayang-generic-jdbc`.

---

## 5. SQL Compatibility Detail

### What `createSqlQuery` currently generates

```sql
SELECT region, amount FROM iceberg.sales.orders WHERE region = 'APAC'
```

### Trino — no changes needed

Trino accepts standard ANSI SQL. The only change needed was removing the
trailing `;` (done). 3-part table names (`catalog.schema.table`) work natively.

### BigQuery — one hook needed

BigQuery requires backtick-quoted identifiers for any column or table name
that is a reserved word. The table name is already handled (user passes
`` `project.dataset.table` `` as a literal string). For column names in
`SELECT`, the current code emits raw names like `region, amount`. This works
as long as no column is a reserved word.

To handle reserved-word columns properly in the future, `BigQueryExecutor`
overrides `quoteIdentifier()` to wrap each field name in backticks:

```sql
-- Before (current)
SELECT region, amount FROM `project.sales.orders`

-- After (with quoteIdentifier override)
SELECT `region`, `amount` FROM `project.sales.orders`
```

---

## 6. Cost Model Rationale

The `cpu` formula in each properties file is `α * rows + β`:

| Engine | α (per-row) | β (fixed startup) | Reasoning |
|---|---|---|---|
| Postgres (existing) | 55 | 380 000 | Single-node, local disk, high per-row overhead |
| GenericJdbc (existing) | 55 | 380 000 | Same as Postgres (placeholder) |
| **Trino** | **10** | **800 000** | Distributed, fast scans; but cluster startup + query planning is expensive |
| **BigQuery** | **5** | **2 000 000** | Massively parallel columnar; ultra-low per-row but very high fixed dispatch cost |

These are **initial estimates** to be tuned after real benchmarks. The key insight
for the optimizer is the *relative* shape:

- Small tables (< ~20k rows): Postgres/GenericJdbc wins (low β)
- Medium tables (20k–500k rows): Trino starts winning
- Large tables (> 500k rows): BigQuery wins (α = 5 becomes dominant)

---

## 7. Implementation Order

1. **Add `quoteIdentifier()` hook** to `GenericJdbcExecutor` (base change, no risk)
2. **Create `TrinoPlatform` + `TrinoExecutor` + `TrinoTableSource`** + properties
3. **Create `BigQueryPlatform` + `BigQueryExecutor` + `BigQueryTableSource`** + properties
4. **Update existing integration tests** to use `TrinoTableSource` / `BigQueryTableSource`
5. **Benchmark and tune α/β** values per engine

---

## 8. What Does NOT Change

- `GenericJdbcPlatform` remains as a generic fallback for any JDBC source
  not explicitly mapped (the current `"trino"` / `"bigquery"` string approach
  still works for quick prototyping)
- `GenericJdbcExecutor`, `GenericJdbcFilterOperator`, `GenericJdbcProjectionOperator`
  are unchanged (only `quoteIdentifier` hook is added)
- `GenericSqlToStreamOperator` is unchanged — it already uses `getPlatformId()`,
  which now resolves to `"trino"` or `"bigquery"` automatically
- The `wayang-jdbc` module (base interfaces) is untouched
