# Data Lake Engine Integration — Design Plan

> Trino is used as the reference implementation throughout.
> The design applies equally to Dremio, BigQuery, and Apache Datafusion.

---

## Background

Apache Wayang is a cross-platform data processing framework. Its optimizer automatically selects which engine (Spark, Flink, Postgres, Java) to run each part of a pipeline on, enabling hybrid execution across platforms.

Current platforms are OLTP databases and batch compute engines. Data lake engines — Trino, Dremio, BigQuery, Datafusion — are not supported. These are distributed SQL engines designed for OLAP on open table formats (Iceberg, Delta Lake) over object storage (S3, GCS).

---

## Project Goal

Add data lake engine backends to Wayang. The project includes:

- **Backend abstraction layer** — `Platform`, `Executor`, `Plugin` that make the new engine visible to Wayang's optimizer and executor
- **Operator mappings** — translate Wayang's logical operators (filter, join, aggregate) into the engine's native execution model (SQL for Trino)
- **Cost estimation** — per-operator load formulas so the optimizer can score the new engine against Spark/Java alternatives

---

## High-Level Approach

### Wayang Architecture

Wayang has three zones. Any new engine must plug into all three.

```
  User API
     │ builds
     ▼
  WayangPlan  (logical, platform-agnostic)
  FilterOp → AggregateOp → CollectOp
     │
     │  OPTIMIZER
     │  - Mappings expand each logical op into platform alternatives
     │  - Cardinality estimates + cost formulas score each alternative
     │  - Picks lowest-cost complete assignment
     │
     ▼
  ExecutionPlan  (physical, one platform per task)
  [new engine stage]  TableScan ──[QueryChannel]──> Filter ──> Collect
  [Spark stage]       ReduceBy ──[RddChannel]──> Sink
     │
     │  EXECUTOR
     │  CrossPlatformExecutor walks stages in order
     │  For each stage: routes to the right Executor (new engine / Spark / Java)
     │  Executor calls operator.evaluate() task by task
     │
     ▼
  Results
```

### What needs to be added (for any new engine)

Every new engine needs one piece in each zone:

```
  OPTIMIZER ZONE
  ┌──────────────────────────────────────────────────────────────┐
  │  Mappings              logical op → engine physical op       │
  │  defaults.properties   cost formula per operator             │
  └──────────────────────────────────────────────────────────────┘

  PLAN ZONE  (the "wires" between tasks in ExecutionPlan)
  ┌──────────────────────────────────────────────────────────────┐
  │  QueryChannel     lazy execution handle (like RddChannel)    │
  │  ResultChannel    materialized output (cross-platform exit)  │
  └──────────────────────────────────────────────────────────────┘

  EXECUTOR ZONE
  ┌──────────────────────────────────────────────────────────────┐
  │  Platform         singleton, owns the engine client/session  │
  │  Executor         walks stage tasks, calls evaluate()        │
  │  Operators        each owns its evaluate() logic             │
  │  Plugin           registers everything above                 │
  └──────────────────────────────────────────────────────────────┘
```

The plugin is the entry point that wires all three zones into Wayang at startup:

```
  [Engine]Plugin
    ├── [Engine]Platform → [Engine]Executor → operators (evaluate())
    ├── Mappings      (FilterOp → [Engine]FilterOp, AggOp → [Engine]AggOp, ...)
    └── Channels      ([Engine]QueryChannel, [Engine]ResultChannel + conversions)
```

### Design Decisions

These are the hardest problems to get right. Getting any of them wrong affects the whole system.

---

#### 1. Operator → SQL translation

The central problem. Wayang's logical operators carry **Java lambdas** — arbitrary code. SQL engines can only execute SQL. There is no automatic lambda→SQL conversion.

#### SQL generation approaches: three options

##### Option 1 — Central Assembler (JDBC template approach)

The executor walks all tasks in the stage, collects a SQL clause from each operator via `createSqlClause()`, then assembles one flat SQL string:

```java
// JdbcExecutor.createSqlString() — real code
"SELECT " + projection + " FROM " + tableName + joins + " WHERE " + conditions
```

> **User burden**: for any operator with a lambda (e.g. filter), the user must provide both a Java implementation and a SQL clause string via `.withSqlImplementation("amount > 100")`. The assembler reads the SQL string — it never inspects the Java lambda.

| | |
|---|---|
| **Pros** | Simple to implement. Clean, flat SQL output — easy to debug. Trino's query planner optimizes flat SQL best. |
| **Cons** | Fixed query shape (`SELECT ... FROM ... WHERE ... JOIN ...`). Can't support `GROUP BY`, `HAVING`, window functions, CTEs — adding them requires modifying the central assembler. Operators are tightly coupled to the assembler's structure. Hard to extend. |
| **Verdict** | Good starting point, but hits a ceiling quickly for OLAP use cases. This is exactly why JDBC template never implemented Aggregate. |

---

##### Option 2 — Per-operator Subquery Wrapping (our current design)

Each operator wraps the previous SQL as a subquery in its own `evaluate()`. No central coordinator.

```java
// TrinoFilterOperator
"SELECT * FROM (" + inputSql + ") WHERE amount > 100"

// TrinoAggregateOperator
"SELECT region, SUM(amount) FROM (...) GROUP BY region"
```

> **User burden**: same as Option 1 — for any operator with a lambda, the user must provide both a Java implementation and a SQL string via `.withSqlImplementation("amount > 100")`. Each operator reads the SQL string from the descriptor and embeds it in its subquery wrapper.

| | |
|---|---|
| **Pros** | Each operator is fully independent — adding a new operator never touches existing code. Supports any SQL construct (GROUP BY, window functions, CTEs). Simple to implement per-operator. Easy to test in isolation (just assert SQL string in, SQL string out). |
| **Cons** | Generates deeply nested subqueries. SQL output is ugly and hard to read. Some engines handle nested subqueries worse than flat SQL (Trino handles them well — it flattens internally, but not all engines do). No cross-operator optimization (e.g., can't merge two adjacent filters into one WHERE). |
| **Verdict** | Best balance of simplicity and flexibility for an initial implementation. Recommended starting point. |

---

##### Option 3 — AST-based (Apache Calcite)

Build an in-memory SQL AST. Each operator modifies the AST by adding nodes (a `Filter` node, `Aggregate` node, etc.). Render the final AST to SQL at the end.

**Good news**: Apache Calcite is **already in the Wayang codebase** — `wayang-api-sql` uses it for SQL parsing and optimization (`SqlNode`, `RelNode`, `RexNode`). No new dependency needed.

```java
// Each operator modifies the RelNode tree (Calcite's logical plan IR)
RelNode scan   = tableSource.toRelNode(cluster);       // TableScan
RelNode filter = relBuilder.filter(scan, condition);   // Filter node added
RelNode agg    = relBuilder.aggregate(filter, groupBy, aggCalls); // Aggregate node

// Render to SQL at TrinoCollect time
SqlNode sql = relToSql.visitChild(0, agg).asStatement();
String query = sql.toSqlString(TrinoSqlDialect.DEFAULT).getSql();
```

| | |
|---|---|
| **Pros** | Clean, flat SQL output — the AST renderer produces optimal, readable SQL. Cross-operator optimization possible (merge adjacent filters, push predicates through joins). Dialect-aware rendering: same AST can render to Trino SQL, BigQuery SQL, Dremio SQL with different `SqlDialect`. Calcite already in codebase — no new dependency. |
| **Cons** | High complexity — Calcite's API is verbose and has a steep learning curve (`RelBuilder`, `RexNode`, `RelDataType`, `SqlDialect` all need to be wired correctly). More code to write and maintain. Overkill if you only need Filter/Project/Aggregate. Schema must be known at plan-build time (Calcite is type-checked). |
| **Verdict** | The right long-term architecture, especially if you want to support multiple SQL engines (Trino, BigQuery, Dremio) from one codebase using dialect-aware rendering. But high upfront cost — only worthwhile if multi-engine support is a firm requirement. |

---

##### Comparison summary

| | Assembler | Subquery wrapping | AST (Calcite) |
|---|---|---|---|
| Implementation effort | Low | Low-medium | High |
| SQL output quality | Clean flat SQL | Ugly nested SQL | Clean flat SQL |
| Operator extensibility | Poor (modify assembler) | Excellent (independent) | Good (add RelNode) |
| Supports GROUP BY / window | No | Yes | Yes |
| Multi-engine dialect support | No | No (dialect hardcoded) | Yes (SqlDialect) |
| Cross-operator optimization | No | No | Yes |
| Already in codebase | Yes (JDBC template) | No | Yes (Calcite) |

**Recommendation**: start with **Option 2 (subquery wrapping)** for the initial implementation — lowest risk, easiest to test, supports all OLAP operators. If multi-engine support (Trino + BigQuery + Dremio from one connector) becomes a requirement, migrate to **Option 3 (Calcite AST)** which gives dialect-aware rendering for free.

---

#### 2. Cost model for Trino

**How existing connectors do it**: each platform defines a `defaults.properties` file with one cost formula per operator. The formula takes input/output cardinalities (`in0`, `out0`) and computes CPU, RAM, disk, and network cost. These coefficients (the `α`/`β` values) were measured empirically by running benchmarks.

```properties
# Postgres tablesource — real formula from wayang-postgres-defaults.properties
wayang.postgres.tablesource.load = {
  "cpu":"${55*out0 + 380000}",   # α=55 per row, β=380000 fixed overhead
  "ram":"0",
  "p":0.9                        # 90% confidence in this estimate
}
# NB: Not measured. ← literally what the comment says in the source

# Spark filter — real formula from wayang-spark-defaults.properties
wayang.spark.filter.load = {
  "cpu":"${500*in0 + 56789}",    # α=500 per row, β=56789 fixed overhead
  "ram":"10000",
  "net":"0",
  "p":0.9
}
```

The optimizer evaluates these formulas at plan time using estimated cardinalities, converts CPU cycles to milliseconds using configured `cpu.mhz` and `cores`, then converts time to cost using `costs.per-ms`. It scores every candidate plan and picks the lowest total cost.

**The Trino challenge**: Trino's actual cost depends heavily on what it's querying underneath:

```
Trino → Iceberg on S3        →  high latency, high throughput (good for large scans)
Trino → live PostgreSQL      →  orders of magnitude different per-row cost
Trino → in-memory tables     →  near-zero latency
```

A single `α` coefficient per operator won't be accurate across all Trino connector types.

**Two approaches**:

- **Black-box heuristics** (start here): treat Trino as a single platform, write formulas like Postgres/Spark did, calibrate via TPC-H benchmarks. Fast to implement, good enough for initial routing decisions. Note: Postgres's own comment says "NB: Not measured" — meaning even existing connectors use rough estimates.

- **Connector-aware cost**: different `defaults.properties` sections per Trino connector type (e.g., `wayang.trino.iceberg.*`, `wayang.trino.postgres.*`). More accurate but requires the user to configure which connector Trino is using.

Getting this wrong means the optimizer routes to Trino when it shouldn't (e.g., tiny datasets faster in Java), or ignores Trino when it would win (e.g., large aggregations on Iceberg). Start with black-box heuristics and refine with benchmarks.

---

#### 3. Result materialization

When Trino finishes, results need to get back into Wayang's pipeline. Two approaches:

**In-memory streaming** (simple, works for small data):
```
Trino → HTTP response → List<Record> in driver JVM → next platform
```
Bottleneck for large results — everything passes through driver memory.

**Shared object storage** (scalable, for large data):
```
Trino → WRITE results to S3/HDFS as Parquet
                │
         FileChannel.Instance holds path
                │
         Next platform (Spark) → READ from S3/HDFS directly
```

This avoids routing large datasets through the driver JVM entirely. The optimizer picks between the two routes via cost formulas — the FileChannel route has higher fixed overhead but much better per-row cost at scale.

Both paths must be registered as `ChannelConversion`s so the optimizer can choose automatically based on estimated cardinality (see Phase 2 — Channel Conversions).


---

## Reference Implementation: Trino

For other engines (Dremio, BigQuery, Datafusion), substitute the engine-specific client and SQL dialect. The structure is identical.

### Directory Structure

```
wayang-platforms/wayang-trino/
└── src/main/java/org/apache/wayang/trino/
      ├── channels/
      │     ├── TrinoQueryChannel.java        ← lazy SQL handle
      │     └── TrinoResultChannel.java       ← materialized rows
      ├── execution/
      │     └── TrinoExecutor.java
      ├── mapping/
      │     ├── TableScanMapping.java
      │     ├── FilterMapping.java
      │     ├── ProjectionMapping.java
      │     ├── AggregateMapping.java
      │     ├── JoinMapping.java
      │     └── SortMapping.java
      ├── operators/
      │     ├── TrinoExecutionOperator.java   ← interface
      │     ├── TrinoTableScanOperator.java
      │     ├── TrinoFilterOperator.java
      │     ├── TrinoProjectOperator.java
      │     ├── TrinoAggregateOperator.java
      │     ├── TrinoJoinOperator.java
      │     ├── TrinoSortOperator.java
      │     ├── TrinoLimitOperator.java
      │     └── TrinoCollectOperator.java     ← action: submits SQL, reads rows
      ├── platform/
      │     └── TrinoPlatform.java
      ├── plugin/
      │     └── TrinoPlugin.java
      └── resources/
            └── wayang-trino-defaults.properties
```

---

### Build Order

Mappings come first — they are the prerequisite for everything else. Until mappings are registered, Trino is completely invisible to the optimizer. With mappings registered (even against stub operators), you can immediately verify the optimizer is choosing Trino by printing the `ExecutionPlan`.

```
Step 1 — Platform skeleton
  TrinoPlatform          singleton, getExecutorFactory() → TrinoExecutor
  TrinoExecutionOperator interface with evaluate() + containsAction()
  TrinoExecutor          stub — just iterates tasks and calls evaluate()
  TrinoPlugin            registers everything; entry point for users

Step 2 — Minimum runnable pipeline (TableScan + Collect)
  TrinoQueryChannel      Instance holds SQL string (lazy handle)
  TrinoResultChannel     Instance holds List<Record> (materialized)
  TrinoTableScanOperator emits initial SELECT * FROM table
  TrinoCollectOperator   submits final SQL to Trino via HTTP, reads rows
  TableScanMapping       logical TableSource → TrinoTableScanOperator
    → at this point: a minimal TableScan → Collect pipeline runs end-to-end

Step 3 — Core pushdown operators + mappings (add one at a time)
  TrinoFilterOperator    wraps SQL with WHERE clause
  FilterMapping          ← register immediately after operator
  TrinoProjectOperator   wraps SQL with SELECT columns
  ProjectionMapping      ← register immediately after operator
  TrinoAggregateOperator wraps SQL with GROUP BY
  AggregateMapping       ← register immediately after operator
  TrinoJoinOperator      combines two SQL inputs with JOIN
  JoinMapping            ← register immediately after operator

Step 4 — Channel conversions (cross-platform handoff)
  TrinoResultChannel → CollectionChannel   (small data, in-memory)
  TrinoResultChannel → FileChannel         (large data, S3/HDFS Parquet)
  FileChannel → TrinoQueryChannel          (re-entry into Trino from file)
  CollectionChannel → TrinoQueryChannel    (tiny data, VALUES subquery)

Step 5 — Cost model
  wayang-trino-defaults.properties
  cost formula per operator (α·n + β), calibrated via TPC-H benchmarks
```

---

### Key Implementation Details

#### How operators compose: per-operator subquery wrapping (Option 2)

Each operator is independent. It reads the SQL string from its input `TrinoQueryChannel`, wraps it as a subquery, and writes the new SQL to its output `TrinoQueryChannel`. No central assembler. No coordination between operators.

Nothing is sent to Trino until `TrinoCollectOperator` fires — everything before is just string manipulation in the JVM.

```
TableScan  →  Filter  →  Aggregate  →  Collect
    │             │            │            │
    ▼             ▼            ▼            ▼
"SELECT *    "SELECT *    "SELECT      submits
 FROM        FROM (...)    region,      final SQL
 orders"     WHERE         SUM(amount)  to Trino
             amount>100"   FROM (...)   → rows back
                           GROUP BY
                           region"
```

Each operator's `evaluate()`:

```java
// Step 1 — TrinoTableScanOperator (source, no input channel)
String sql = "SELECT * FROM " + catalog + "." + schema + "." + table;
outputChannel.setSql(sql);

// Step 2 — TrinoFilterOperator
//   reads:  "SELECT * FROM orders"
//   wraps:  adds WHERE as outer subquery
String inputSql = inputChannel.getSql();
String sql = "SELECT * FROM (" + inputSql + ") WHERE " + whereClause;
outputChannel.setSql(sql);

// Step 3 — TrinoProjectOperator
//   reads:  "SELECT * FROM (...) WHERE amount > 100"
//   wraps:  narrows SELECT columns
String columns = String.join(", ", fieldNames);  // same logic as JdbcProjectionOperator
String sql = "SELECT " + columns + " FROM (" + inputSql + ")";
outputChannel.setSql(sql);

// Step 4 — TrinoAggregateOperator
//   reads:  previous SQL
//   wraps:  adds GROUP BY as outer subquery
String sql = "SELECT " + groupKeys + ", " + aggFunctions
           + " FROM (" + inputSql + ") GROUP BY " + groupKeys;
outputChannel.setSql(sql);

// Step 5 — TrinoJoinOperator (two input channels: left + right)
//   reads:  two independent SQL strings
//   wraps:  combines with JOIN
String sql = "SELECT * FROM (" + leftSql + ") l"
           + " JOIN (" + rightSql + ") r ON l." + key + " = r." + key;
outputChannel.setSql(sql);

// Step 6 — TrinoCollectOperator (containsAction() = true)
//   reads:  final composed SQL
//   fires:  ONE HTTP call to Trino — this is the only point execution happens
String finalSql = inputChannel.getSql();
List<Record> rows = trinoExecutor.getClient().query(finalSql);
outputChannel.setRows(rows);
```

Because each operator wraps independently, adding a new operator (e.g. `TrinoWindowOperator`) never touches any existing code — just write the new `evaluate()` and register its mapping.

#### Channel conversions: cost-driven, not threshold-driven

The optimizer picks between conversions automatically via `estimateConversionCost(cardinality)`. No explicit size check in code — the cost formulas naturally favor in-memory for small data and file-based for large data.

| From | To | How | Good for |
|---|---|---|---|
| `TrinoResultChannel` | `CollectionChannel` | trivial cast | small results |
| `TrinoResultChannel` | `RddChannel` | `sc.parallelize(rows)` | small→Spark handoff |
| `TrinoResultChannel` | `FileChannel` (HDFS/S3) | write Parquet | large results |
| `FileChannel` (HDFS/S3) | `TrinoQueryChannel` | Trino reads external table | large→Trino re-entry |
| `CollectionChannel` | `TrinoQueryChannel` | rows as `VALUES` subquery | tiny data only |

> **Important**: both in-memory and file-based paths must be registered. If only `CollectionChannel` is registered, the optimizer has no alternative for large data and will always collect to driver JVM — causing OOM.

#### Cross-platform Aggregate

| Scenario | Works? | Notes |
|---|---|---|
| Pure Trino: TableScan → Filter → Aggregate → Collect | ✓ | One SQL, full pushdown |
| Trino Aggregate after cross-platform (small data) | ✓ | VALUES subquery |
| Trino Aggregate after cross-platform (large data) | ⚠ | Use FileChannel — Trino reads external table |

#### Cost model

```properties
wayang.trino.TrinoTableScanOperator.load = { ... α * numRows ... }
wayang.trino.TrinoFilterOperator.load    = { ... α * inputRows ... }
wayang.trino.TrinoAggregateOperator.load = { ... α * inputRows ... }
wayang.trino.init.ms = 200   # low startup cost vs. Spark cluster spin-up
```

Calibrate `α` and `β` using TPC-H benchmarks after the executor is running.

---

## What You Build vs. What Wayang Gives for Free

| You build | Wayang gives for free |
|---|---|
| Platform, Plugin, Executor | `CrossPlatformExecutor` scheduling |
| QueryChannel, ResultChannel | Channel conversion graph routing |
| ExecutionOperator interface | `ExecutionStage` / `ExecutionTask` wiring |
| All operators (`evaluate()` logic) | Optimizer enumeration + cost scoring |
| All mappings (logical → physical) | Re-optimization feedback loop |
| `defaults.properties` formulas | Cardinality estimation infrastructure |
| Engine HTTP/client calls | Cross-platform bridge operator injection |

---

## Testing Strategy

### Unit tests
- Each operator: given a known SQL string in → assert the SQL out is correct
- No real engine connection needed for most operator tests

### Integration tests
- Spin up Trino via Docker: `docker run -p 8080:8080 trinodb/trino`
- `TrinoTableScanOperator` → `TrinoFilterOperator` → `TrinoCollectOperator` end-to-end
- Cross-platform: Trino filter → Spark reduceBy (validates channel conversion)

```bash
docker run -d --name trino -p 8080:8080 trinodb/trino
curl http://localhost:8080/v1/info   # wait for ready
```

---

## Open Questions

1. **SQL dialect**: each engine has quirks (Trino: `UNNEST`, BigQuery: `UNNEST` + array syntax). How much of the SQL builder is shared vs. engine-specific?
2. **Result format**: start with `List<Record>` rows or invest in Apache Arrow from day one for columnar efficiency?
3. **Authentication**: which auth mechanism does each target engine use? (Trino: basic/OAuth2, BigQuery: service account, Dremio: PAT)
4. **Catalog routing**: set at the platform level (per `TrinoPlatform` config) or per `TableScanOperator`?
5. **Cost calibration**: need engine-specific benchmarks (TPC-H) to set meaningful `α`/`β` values in `defaults.properties`.
