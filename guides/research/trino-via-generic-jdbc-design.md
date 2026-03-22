# Trino via GenericJdbc — Design Document

> Connect Trino to Wayang by leveraging the existing `wayang-generic-jdbc` module.
> No new platform, no new executor — just configuration, a JDBC driver, and targeted fixes.

---

## 1. Why this approach

`wayang-generic-jdbc` was built exactly for this use case: plug any JDBC-compatible database into Wayang by providing a `jdbcName` and a set of `wayang.{jdbcName}.jdbc.*` properties. It already ships with:

- A `GenericJdbcPlatform` that resolves connections dynamically by name
- An executor (`GenericJdbcExecutor`) that assembles SQL from operator clauses
- Operators for **TableScan**, **Filter**, and **Projection**
- Mappings that wire logical operators to these physical operators
- A channel conversion (`SqlQueryChannel → StreamChannel`) that executes the assembled SQL and streams `Record`s back
- Cost formulas in `wayang-genericjdbc-defaults.properties`

Trino exposes a standard JDBC interface (`io.trino:trino-jdbc`). This means we can connect Trino to Wayang **without writing a single new operator or executor** — just by setting configuration properties.

---

## 2. How it works end-to-end

### Data flow

```
User API
  │
  │  WayangPlan: TableSource → FilterOp → MapOp(projection) → Sink
  │
  ├──── OPTIMIZER ────────────────────────────────────────────────────
  │  FilterMapping:      FilterOp(Record) → GenericJdbcFilterOperator
  │  ProjectionMapping:  MapOp(Record→Record, ProjectionDescriptor) → GenericJdbcProjectionOperator
  │  (only if filter has .withSqlImplementation(...))
  │
  ├──── EXECUTION PLAN ──────────────────────────────────────────────
  │  [GenericJdbc stage]
  │    GenericJdbcTableSource ──SqlQueryChannel──> GenericJdbcFilterOp
  │       ──SqlQueryChannel──> GenericJdbcProjectionOp ──SqlQueryChannel──>
  │
  │  [Java stage]
  │    GenericSqlToStreamOperator ──StreamChannel──> Sink
  │
  ├──── EXECUTOR ────────────────────────────────────────────────────
  │  GenericJdbcExecutor.execute(stage):
  │    1. Walk tasks: TableSource → Filter → Projection
  │    2. Collect SQL clauses:
  │         tableName  = "iceberg.sales.orders"
  │         conditions = ["amount > 100"]
  │         projection = "region, product"
  │    3. Assemble: "SELECT region, product FROM iceberg.sales.orders WHERE amount > 100;"
  │    4. Store in SqlQueryChannel.Instance (lazy — nothing sent to Trino yet)
  │
  │  GenericSqlToStreamOperator.evaluate():
  │    1. Read jdbcName from SqlQueryChannel.Instance
  │    2. Resolve connection: wayang.trino.jdbc.url → jdbc:trino://localhost:8080
  │    3. Execute SQL via JDBC → ResultSet → Stream<Record>
  │    4. Output to StreamChannel → next stage
  │
  ▼
Results
```

### Connection path

```
GenericJdbcTableSource("trino", "iceberg.sales.orders", "order_id", "region", ...)
                         │
                         ▼ jdbcName = "trino"
                   Configuration lookup:
                     wayang.trino.jdbc.url        = jdbc:trino://localhost:8080
                     wayang.trino.jdbc.user       = admin
                     wayang.trino.jdbc.password   = (empty)
                     wayang.trino.jdbc.driverName = io.trino.jdbc.TrinoDriver
                         │
                         ▼
                   DatabaseDescriptor → DriverManager.getConnection(url, user, password)
                         │
                         ▼
                   Trino JDBC connection
```

---

## 3. Supported operators (out of the box)

These operators already exist and work with Trino via JDBC without any code changes:

| Wayang logical op | GenericJdbc physical op | SQL clause | Notes |
|---|---|---|---|
| `TableSource` | `GenericJdbcTableSource` | `FROM {table}` | User passes fully qualified table name: `iceberg.sales.orders` |
| `FilterOperator` | `GenericJdbcFilterOperator` | `WHERE {sql}` | Requires `.withSqlImplementation("amount > 100")` on the predicate |
| `MapOperator` (with `ProjectionDescriptor`) | `GenericJdbcProjectionOperator` | `SELECT {cols}` | Projection only — not arbitrary map |

### Assembled SQL shape

The executor uses the **central assembler** pattern (Option 1 from the original design doc):

```sql
SELECT {projection} FROM {table} WHERE {condition1} AND {condition2};
```

This is a flat query — no subqueries, no nesting. Trino's optimizer handles this very efficiently.

### What is NOT supported

| Operator | Why not | Fallback |
|---|---|---|
| **Join** | `GenericJdbcExecutor` throws `Unsupported JDBC execution task` for anything other than Filter/Projection | Falls back to Java/Spark |
| **Aggregate** (GROUP BY) | No operator exists in `wayang-generic-jdbc` | Falls back to Java/Spark |
| **Sort** (ORDER BY) | No operator exists | Falls back to Java/Spark |
| **Arbitrary map (UDF)** | Java lambdas can't be converted to SQL | Falls back to Java/Spark |

The fallback is automatic — if no GenericJdbc mapping exists for an operator, the optimizer simply routes it to another platform.

---

## 4. What needs to change

### 4.1 Add Trino JDBC driver dependency

Add to `wayang-generic-jdbc/pom.xml`:

```xml
<dependency>
    <groupId>io.trino</groupId>
    <artifactId>trino-jdbc</artifactId>
    <version>435</version>
</dependency>
```

**Alternative**: add it only to the test module or the user's application classpath (keeps the generic-jdbc module engine-agnostic).

### 4.2 Trino configuration properties

Users set these in their `Configuration` (or `wayang.properties`):

```properties
wayang.trino.jdbc.url        = jdbc:trino://localhost:8080
wayang.trino.jdbc.user       = admin
wayang.trino.jdbc.password   =
wayang.trino.jdbc.driverName = io.trino.jdbc.TrinoDriver
```

### 4.3 Fix: `GenericJdbcExecutor.connection` is hardcoded to `null`

```java
// Current code (line 66)
private final Connection connection = null;
```

This field is passed to `createSqlClause(connection, compiler)`. It happens to work because **none of the three operators use the `connection` parameter** — they only use the `FunctionCompiler`:

| Operator | `createSqlClause(connection, compiler)` implementation |
|---|---|
| `JdbcTableSource` | `return this.getTableName();` — ignores both params |
| `JdbcFilterOperator` | `return compiler.compile(this.predicateDescriptor);` — ignores `connection` |
| `JdbcProjectionOperator` | `return String.join(", ", fieldNames);` — ignores both params |

So the `null` connection is **currently benign**. But it's still a bug that should be fixed for safety, especially if future operators need the connection (e.g., for `DESCRIBE TABLE` metadata queries).

**Fix**: create the connection in the constructor (restore the commented-out line):

```java
public GenericJdbcExecutor(GenericJdbcPlatform platform, Job job) {
    super(job.getCrossPlatformExecutor());
    this.platform = platform;
    // Restore connection initialization — needs jdbcName at this point
}
```

**Complication**: the executor doesn't know the `jdbcName` at construction time (it comes from the `GenericJdbcTableSource` in the stage). This is a design limitation. The current workaround (connection created in `GenericSqlToStreamOperator` instead) is functional and can remain as-is for now.

### 4.4 Fix: semicolon in assembled SQL

```java
// GenericJdbcExecutor.createSqlQuery() line 203
sb.append(';');
```

Trino JDBC does **not** accept trailing semicolons. This will cause a parse error.

**Fix**: remove the semicolon:

```java
// sb.append(';');  // Remove — Trino (and most JDBC drivers) reject trailing semicolons
```

### 4.5 Trino-specific: fully qualified table names

Trino requires 3-part table names: `catalog.schema.table`.

The `GenericJdbcTableSource` stores the table name as a single string, so the user must pass the full path:

```java
new GenericJdbcTableSource("trino", "iceberg.sales.orders", "order_id", "region", "amount")
```

This works as-is — no code change needed.

### 4.6 Cardinality estimation: `SELECT count(*)` compatibility

`GenericJdbcTableSource.getCardinalityEstimator()` runs:

```sql
SELECT count(*) FROM iceberg.sales.orders;
```

This is valid Trino SQL and works correctly. **However**, it creates a JDBC connection to Trino at **optimization time** (not execution time), which adds latency (~1–2 seconds for Trino cold start). This is acceptable — Postgres does the same thing.

---

## 5. User code example

```java
Configuration config = new Configuration();
config.setProperty("wayang.trino.jdbc.url", "jdbc:trino://localhost:8080");
config.setProperty("wayang.trino.jdbc.user", "admin");
config.setProperty("wayang.trino.jdbc.password", "");
config.setProperty("wayang.trino.jdbc.driverName", "io.trino.jdbc.TrinoDriver");

WayangContext ctx = new WayangContext(config)
    .withPlugin(Java.basicPlugin())
    .withPlugin(GenericJdbc.plugin());

Collection<Record> results = new ArrayList<>();

// Table source — fully qualified Trino table name
TableSource source = new GenericJdbcTableSource(
    "trino",                          // jdbcName
    "iceberg.sales.orders",           // table (catalog.schema.table)
    "order_id", "region", "amount"    // column names
);

// Filter — must provide SQL implementation alongside Java lambda
FilterOperator<Record> filter = new FilterOperator<>(
    new PredicateDescriptor<>(
        record -> ((Number) record.getField(2)).doubleValue() > 1000.0,  // Java
        Record.class
    ).withSqlImplementation("amount > 1000")                             // SQL for Trino
);

// Projection
MapOperator<Record, Record> project = new MapOperator<>(
    new ProjectionDescriptor<>(Record.class, Record.class, "region", "amount"),
    DataSetType.createDefault(Record.class),
    DataSetType.createDefault(Record.class)
);

LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(
    results, Record.class
);

// Wire the plan
source.connectTo(0, filter, 0);
filter.connectTo(0, project, 0);
project.connectTo(0, sink, 0);

ctx.execute("Trino via GenericJdbc", new WayangPlan(sink));
// Results now in `results` list
```

### What the optimizer generates

```
GenericJdbc stage:
  GenericJdbcTableSource("trino", "iceberg.sales.orders", ...)
  → GenericJdbcFilterOperator("amount > 1000")
  → GenericJdbcProjectionOperator("region", "amount")

Java stage:
  GenericSqlToStreamOperator → LocalCallbackSink
```

### What SQL is sent to Trino

```sql
SELECT region, amount FROM iceberg.sales.orders WHERE amount > 1000
```

One query. Full pushdown of filter + projection. No intermediate data movement.

---

## 6. Cross-platform example (Trino + Spark)

```java
// Source from Trino
TableSource trinoSource = new GenericJdbcTableSource(
    "trino", "iceberg.sales.orders", "region", "amount"
);

// Filter pushed to Trino
FilterOperator<Record> filter = new FilterOperator<>(
    new PredicateDescriptor<>(
        r -> ((Number) r.getField(1)).doubleValue() > 100, Record.class
    ).withSqlImplementation("amount > 100")
);

// ReduceByKey — no SQL mapping exists → optimizer routes to Spark/Java
ReduceByOperator<Record> reduce = new ReduceByOperator<>(
    record -> record.getField(0),           // key = region
    (a, b) -> { /* sum amounts */ },
    Record.class
);

trinoSource → filter → reduce → sink
```

The optimizer produces:

```
GenericJdbc stage:   TableSource → Filter   →  SQL: "SELECT * FROM ... WHERE amount > 100"
   ──[SqlQueryChannel]──>  GenericSqlToStreamOperator  ──[StreamChannel]──>
Spark/Java stage:    ReduceByKey → Sink
```

The Trino stage pushes filter to the data lake engine. Results stream back via JDBC into Java memory, then continue into Spark/Java for the reduce.

---

## 7. Limitations

| Limitation | Impact | Workaround |
|---|---|---|
| No `GROUP BY` pushdown | Aggregations run in Java/Spark, not Trino | Future: add `GenericJdbcAggregateOperator` |
| No `JOIN` pushdown | Joins run in Java/Spark even when both tables are on Trino | Future: fix `GenericJdbcExecutor` join support |
| No `ORDER BY` / `LIMIT` | Sorting runs in Java/Spark | Future: add sort operator |
| User must provide `.withSqlImplementation()` for predicates | Extra boilerplate | This is how all JDBC connectors work in Wayang |
| Single executor, single platform ID | All GenericJdbc sources share one `GenericJdbcPlatform` instance — optimizer can't distinguish Trino from Postgres for cost scoring | See Section 8 |
| Large result sets pass through driver JVM | `GenericSqlToStreamOperator` streams via JDBC — memory pressure for huge results | Future: add `FileChannel` conversion |

---

## 8. Future improvements

### 8.1 Extend operators

Add to `wayang-generic-jdbc`:

| Operator | SQL clause | Effort |
|---|---|---|
| `GenericJdbcAggregateOperator` | `GROUP BY` + aggregate functions | Medium — requires new operator + mapping + change to `GenericJdbcExecutor.createSqlQuery()` |
| `GenericJdbcJoinOperator` | `JOIN ... ON ...` | Medium — executor already collects join clauses in `JdbcExecutor` parent; `GenericJdbcExecutor` just needs to stop throwing |
| `GenericJdbcSortOperator` | `ORDER BY` + `LIMIT` | Low |

Each new operator benefits **all** GenericJdbc backends (Postgres, MySQL, Trino) simultaneously.

### 8.2 Trino-specific cost model

Override `wayang-genericjdbc-defaults.properties` at runtime:

```properties
# Trino has lower per-row cost than Postgres for large scans (columnar format)
wayang.genericjdbc.tablesource.load = {
  "cpu":"${20*out0 + 500000}",
  "ram":"0", "p":0.8
}
```

Or introduce per-`jdbcName` cost overrides (requires core changes).

### 8.3 Dedicated `wayang-trino` module

If Trino needs capabilities beyond JDBC's central assembler (subquery wrapping, window functions, `CREATE TABLE AS SELECT` for result materialization), fork into a dedicated `wayang-trino` module following the Spark pattern — as described in the original design doc (`trino-connector-impl-plan.md`).

---

## 9. Implementation plan

### Phase 1 — Minimal viable Trino integration (no code changes)

1. Add Trino JDBC driver to classpath (user's `pom.xml` or `wayang-generic-jdbc/pom.xml`)
2. Set `wayang.trino.jdbc.*` properties
3. Use `GenericJdbcTableSource("trino", "catalog.schema.table", columns...)`
4. Run a TableScan → Filter → Projection → Collect pipeline end-to-end

**Deliverable**: a working integration test that queries Trino Iceberg tables via `GenericJdbc.plugin()`.

### Phase 2 — Bug fixes

1. Remove trailing semicolon from `GenericJdbcExecutor.createSqlQuery()`
2. (Optional) Restore `connection` initialization or document the workaround

### Phase 3 — Integration test against local Trino

Leverage the `trino-setup/` Docker stack (already created):

```java
// TrinoGenericJdbcIT.java
@Test void testTrinoFilterProjection() {
    Configuration config = new Configuration();
    config.setProperty("wayang.trino.jdbc.url", "jdbc:trino://localhost:8080");
    config.setProperty("wayang.trino.jdbc.user", "admin");
    config.setProperty("wayang.trino.jdbc.driverName", "io.trino.jdbc.TrinoDriver");

    WayangContext ctx = new WayangContext(config)
        .withPlugin(Java.basicPlugin())
        .withPlugin(GenericJdbc.plugin());

    List<Record> results = new ArrayList<>();
    TableSource source = new GenericJdbcTableSource("trino", "iceberg.sales.orders",
        "order_id", "region", "amount");
    FilterOperator<Record> filter = new FilterOperator<>(
        new PredicateDescriptor<>(r -> true, Record.class)
            .withSqlImplementation("amount > 1000")
    );
    // ... wire plan, execute, assert results ...
}
```

### Phase 4 — Operator extensions (optional)

Add `GenericJdbcAggregateOperator`, `GenericJdbcSortOperator` if needed.

---

## 10. Module dependency map

```
wayang-generic-jdbc
  ├── depends on: wayang-jdbc-template   (JdbcPlatformTemplate, SqlQueryChannel, JdbcExecutionOperator, ...)
  ├── depends on: wayang-basic           (Record, TableSource, FilterOperator, ProjectionDescriptor, ...)
  ├── depends on: wayang-java            (JavaExecutor, StreamChannel — for GenericSqlToStreamOperator)
  └── runtime:    io.trino:trino-jdbc    (Trino JDBC driver — user provides)

User's app
  ├── wayang-generic-jdbc
  ├── wayang-java (or wayang-spark)
  └── io.trino:trino-jdbc:435
```

No new Wayang modules. No new classes. Configuration + driver + bug fix.

---

## Appendix: Key source files

| File | Role |
|---|---|
| `GenericJdbcPlatform.java` | Singleton platform, resolves JDBC config by `jdbcName`, creates `GenericJdbcExecutor` |
| `GenericJdbcExecutor.java` | Walks stage tasks, collects SQL clauses, assembles flat query |
| `GenericJdbcTableSource.java` | Stores `jdbcName` + table name, provides cardinality estimation via `SELECT count(*)` |
| `GenericJdbcFilterOperator.java` | Thin wrapper over `JdbcFilterOperator` → `createSqlClause()` returns `predicateDescriptor.getSqlImplementation()` |
| `GenericJdbcProjectionOperator.java` | Thin wrapper over `JdbcProjectionOperator` → `createSqlClause()` returns `String.join(", ", fieldNames)` |
| `GenericSqlToStreamOperator.java` | Creates JDBC connection from `jdbcName`, executes SQL, streams `Record`s |
| `FunctionCompiler.java` (jdbc-template) | `compile(predicate)` → literally returns `descriptor.getSqlImplementation()` |
| `GenericJdbcPlugin.java` | Registers mappings + channel conversions |
| `GenericChannelConversions.java` | `SqlQueryChannel → StreamChannel` via `GenericSqlToStreamOperator` |
| `Mappings.java` | `FilterMapping` + `ProjectionMapping` |
| `wayang-genericjdbc-defaults.properties` | Cost formulas: `α*n + β` for tablesource, filter, projection, sqltostream |
