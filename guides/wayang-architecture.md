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
  - Unless required by applicable law or agreed to in writing, software
  - distributed under the License is distributed on an "AS IS" BASIS,
  - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  - See the License for the specific language governing permissions and
  - limitations under the License.
  -->

# Apache Wayang — Architecture Overview

This document explains how Apache Wayang works end-to-end: how all its
components are composed together, and what happens at each stage from the user
writing a pipeline to actual distributed execution.

For platform-specific deep dives, see:
- `guides/spark-connector-deep-dive.md`
- `guides/postgres-connector-deep-dive.md`

---

## 1. What Is Wayang?

Wayang is a **cross-platform data processing framework**. The user writes a
pipeline once using a logical API (`DataQuanta`), and Wayang automatically
decides which execution engine(s) to run it on — Spark, Java Streams, Postgres,
Flink, etc. — and can split the pipeline across multiple engines.

```
User writes:  data.filter(...).map(...).reduceBy(...).collect()
                                  │
                    Wayang decides: run filter on Postgres (pushdown),
                                    run reduceBy on Spark (distributed)
                                  │
                    Result returned to user
```

---

## 2. The Two-Layer Plan Model

Wayang keeps two separate representations of the pipeline:

```
Logical Plan (WayangPlan)           Physical Plan (ExecutionPlan)
─────────────────────────           ──────────────────────────────
FilterOperator                  →   SparkFilterOperator
ReduceByOperator                →   SparkReduceByOperator
TextFileSource                  →   SparkTextFileSource

Platform-agnostic                   Platform-specific
"what to do"                        "how and where to do it"
Written by the user                 Built by the optimizer
```

The optimizer's job is to transform the logical plan into the best physical
plan by applying mappings, estimating costs, and assigning channels.

---

## 3. Four Core Concepts (per platform connector)

Every platform connector (Spark, Flink, JDBC, etc.) is built from the same
four building blocks. Think of it like plumbing:

```
[Source] ──── channel ──── [Operator] ──── channel ──── [Operator] ──── channel ──── [Sink]
               pipe                          pipe
```

| Package | Role | Data or Process? | When does it run? |
|---|---|---|---|
| `channels/` | The **pipes** — typed containers that carry data between operators | Data-focused | Always |
| `operators/` | The **valves** — units of computation that transform data | Process-focused | Execution time |
| `mapping/` | The **rulebook** — tells the optimizer which operator class to use | Plan-focused | Optimization time |
| `compiler/` | The **adapter kit** — converts user lambdas into engine-callable functions | Glue | Execution time (inside operators) |

### Channels — The Pipes

A channel is a typed container that carries data between operators. It does not
process data; it just holds a reference to it.

```
Spark:   RddChannel        → holds a JavaRDD<T> reference
Spark:   DatasetChannel    → holds a Spark Dataset<T>
Spark:   BroadcastChannel  → holds a Spark broadcast variable
JDBC:    SqlQueryChannel   → holds a SQL string
Java:    CollectionChannel → holds a Java List<T>
Common:  FileChannel       → holds an HDFS file path
```

`ChannelConversions` defines how to automatically convert between channel types.
The optimizer inserts conversion operators automatically when two platforms need
to pass data to each other:

```
RddChannel ──[SparkCollectOperator]──► CollectionChannel   (Spark → Java)
CollectionChannel ──[SparkCollectionSource]──► RddChannel  (Java → Spark)
SqlQueryChannel ──[SqlToStreamOperator]──► StreamChannel   (JDBC → Java)
```

### Mappings — The Optimizer's Rulebook

A mapping is a substitution rule: "when you see logical operator X, replace it
with physical operator Y on platform Z." Each mapping file = one rule.

```java
// FilterMapping.java (Spark)
// Rule: find any FilterOperator → replace with SparkFilterOperator
SubplanPattern: FilterOperator (any predicate)
Replacement:    new SparkFilterOperator(matchedOperator)
Platform:       SparkPlatform
```

Mappings run **only during optimization**. There are 30+ for Spark, 3 for
JDBC (filter, projection, join — with a SQL gate).

### Operators — The Valves

Each operator is a unit of computation. It pulls data from input channels,
processes it, and pushes results to output channels. The core method is
`evaluate(inputs, outputs, executor)`.

```
Source operators:       create new data (sc.textFile(), JDBC query)
Transformation ops:     transform data (rdd.filter(), rdd.reduceByKey())
Sink/Action operators:  output or trigger real execution (rdd.collect())
```

### Compiler — The Lambda Adapter

Spark requires `org.apache.spark.api.java.function.Function<I,O>`. Users write
plain Java lambdas (`java.util.function.Function<I,O>`). These are different
types. The compiler wraps the user's lambda in a thin adapter:

```java
// MapFunctionAdapter.java — the entire body is one line
public O call(I input) {
    return this.javaFunction.apply(input);  // just delegates
}
```

The operator calls the compiler, then passes the result to the Spark API:

```
Operator uses compiler:
    predicateDescriptor (holds user lambda)
        → compiler.compile()
        → PredicateAdapter (implements Spark Function<T,Boolean>)
        → rdd.filter(filterFunction)   ← Spark can now call it
```

### Dependency Between the Three (Mapping / Operator / Compiler)

```
Mapping  ──creates──►  Operator  ──uses──►  Compiler
   │                      │
   │ (optimization time)  │ (execution time)
   ▼                      ▼
rewrites the plan     calls rdd.filter() etc.
```

- Mapping and Compiler don't know about each other.
- The Operator is the bridge — created by Mapping, uses Compiler.

---

## 4. The Two Composition Points

### Composition Point 1: `SparkBasicPlugin` — setup time

```java
// SparkBasicPlugin.java
public Collection<Mapping> getMappings() {
    return Mappings.BASIC_MAPPINGS;        // ← all 30+ mapping rules
}
public Collection<ChannelConversion> getChannelConversions() {
    return ChannelConversions.ALL;         // ← all channel conversion rules
}
public Collection<Platform> getRequiredPlatforms() {
    return Arrays.asList(SparkPlatform.getInstance(), JavaPlatform.getInstance());
}
```

Called once when the user writes `withPlugin(Spark.basicPlugin())`. Hands the
optimizer its entire rulebook. No operators or compiler involved yet.

### Composition Point 2: `SparkExecutor` — execution time

```java
// SparkExecutor.java
public FunctionCompiler compiler = new FunctionCompiler();  // ← created here

// For each operator task:
cast(task.getOperator()).evaluate(
        toArray(inputChannelInstances),   // input channels
        outputChannelInstances,           // output channels (blank, to be filled)
        this,                             // ← SparkExecutor (carries compiler + sc)
        producerOperatorContext
);
```

The executor is the runtime heart. It holds the compiler and passes itself into
every `evaluate()` call, giving each operator access to:
- `this.sc` — for Spark API calls (`sc.textFile()`, etc.)
- `this.getCompiler()` — to wrap lambdas into Spark functions

---

## 5. The Orchestrator: `Job.doExecute()`

`Job` is the single class that composes everything. It runs a 4-step pipeline:

```
Job.doExecute()
    │
    ├── 1. prepareWayangPlan()          ← applies Mappings to logical plan
    ├── 2. estimateKeyFigures()         ← runs cardinality estimators
    ├── 3. createInitialExecutionPlan() ← enumerates plans, picks best, assigns Channels
    └── 4. execute(executionPlan)       ← runs Operators via SparkExecutor (uses Compiler)
```

### Step 1 — Apply Mappings (`prepareWayangPlan`)

```java
// Job.java
private void prepareWayangPlan() {
    final Collection<PlanTransformation> transformations = this.gatherTransformations();
    this.wayangPlan.applyTransformations(transformations);
    // FilterOperator  → SparkFilterOperator
    // ReduceByOperator → SparkReduceByOperator
}

private Collection<PlanTransformation> gatherTransformations() {
    // reads all registered mappings from the plugin
    return this.configuration.getMappingProvider().provideAll().stream()
            .flatMap(mapping -> mapping.getTransformations().stream())
            ...
}
```

### Step 2 — Estimate Cardinalities (`estimateKeyFigures`)

```java
// Job.java
private void estimateKeyFigures() {
    this.cardinalityEstimatorManager = new CardinalityEstimatorManager(
            this.wayangPlan, this.optimizationContext, this.configuration);
    this.cardinalityEstimatorManager.pushCardinalities();
    // propagates estimated row counts through every operator in the plan
}
```

Each operator has a `CardinalityEstimator`. For JDBC sources this runs
`SELECT count(*)` against the database. For others it uses formulas or
propagates from upstream estimates. These row counts drive the cost model.

### Step 3 — Pick Best Plan + Assign Channels (`createInitialExecutionPlan`)

```java
// Job.java
private ExecutionPlan createInitialExecutionPlan() {
    // 1. Enumerate all possible physical plans
    final PlanEnumeration enumeration = planEnumerator.enumerate(true);

    // 2. Pick cheapest plan using cost model
    this.pickBestExecutionPlan(executionPlans, ...);
    //   costModel computes: (cardinality × load formula) → time → cost
    //   picks lowest cost across all plan combinations

    // 3. Assign channels and split into stages
    final ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.createFrom(this.planImplementation);
    final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
    // Channels are wired here: RddChannel between Spark ops,
    // CollectionChannel at Spark→Java boundary,
    // auto-conversion operators inserted from ChannelConversions
}
```

### Step 4 — Execute (`execute`)

```java
// Job.java
private boolean execute(ExecutionPlan executionPlan, int executionId) {
    this.crossPlatformExecutor = new CrossPlatformExecutor(this, instrumentation);
    // CrossPlatformExecutor walks the plan stage by stage
    // For each Spark stage → creates SparkExecutor → calls operator.evaluate()
}
```

---

## 6. End-to-End Workflow

```
User:  new WayangContext().withPlugin(Spark.basicPlugin())
                │
                ▼
        SparkBasicPlugin
            getMappings()           → optimizer gets all mapping rules (FilterMapping, etc.)
            getChannelConversions() → optimizer gets channel conversion rules (RddChannel↔Collection)
            getRequiredPlatforms()  → registers SparkPlatform, JavaPlatform


User:  plan.execute()
                │
                ▼
        Job.doExecute()
            │
            ├── 1. prepareWayangPlan()
            │       gatherTransformations()           ← reads all Mappings
            │       wayangPlan.applyTransformations()
            │           FilterOperator      → SparkFilterOperator
            │           ReduceByOperator    → SparkReduceByOperator
            │           TextFileSource      → SparkTextFileSource
            │
            ├── 2. estimateKeyFigures()
            │       CardinalityEstimatorManager.pushCardinalities()
            │           estimates row counts for each operator
            │           (uses SELECT count(*) for JDBC, formulas for others)
            │
            ├── 3. createInitialExecutionPlan()
            │       PlanEnumerator.enumerate()        ← generates all platform combos
            │       costModel.pickBest()              ← picks cheapest plan
            │       ExecutionPlan.createFrom()        ← assigns Channels between operators
            │           RddChannel between Spark operators
            │           CollectionChannel at Spark→Java boundary
            │           auto-inserts SparkCollectOperator (from ChannelConversions)
            │
            └── 4. execute(executionPlan)
                    CrossPlatformExecutor
                        └── for each stage:
                              SparkExecutor
                                  ├── sc = SparkContext.getOrCreate()
                                  ├── compiler = new FunctionCompiler()
                                  └── for each task:
                                        operator.evaluate(inputCh, outputCh, sparkExecutor)
                                            │
                                            ├── compiler.compile(lambda)    ← Compiler
                                            │       → wraps Java lambda as Spark Function
                                            │
                                            ├── inputCh.provideRdd()        ← Channel (read)
                                            ├── rdd.filter(fn)              ← Spark API (lazy)
                                            └── outputCh.accept(resultRdd)  ← Channel (write)

                                  [next operator's inputCh = previous outputCh]
                                  [when SparkCollectOperator runs: rdd.collect() → actual execution]
```

---

## 7. Runtime Feedback Loop

After execution, Wayang records actual runtime data in two places:

| Scope | Where stored | Who uses it | Automatic? |
|---|---|---|---|
| Within current job | In-memory (`CrossPlatformExecutor`) | Optimizer re-plans later stages mid-job | Yes |
| Across future jobs | JSON file on disk (`CardinalityRepository`) | Developers tune `.properties` formulas | No — manual |

The within-job feedback works like this:

```
Stage 1: SparkFilterOperator runs
    → actual output: 50,000 records
    → optimizer estimated: 200,000 records (wrong)
    → optimizer injects real count for Stage 2 planning
Stage 2: optimizer re-plans using corrected cardinality
    → may pick different platform or different operator
```

The cross-job feedback is manual: the JSON log is read by developers who
then tune the cost formulas in `wayang-spark-defaults.properties`.

---

## 8. Optimizer Interaction Diagram

This diagram shows which components feed into the optimizer, which it produces,
and which consume its output — and at exactly which stage each interaction occurs.

```
╔══════════════════════════════════════════════════════════════════════════════════╗
║                              JOB LIFECYCLE                                       ║
╚══════════════════════════════════════════════════════════════════════════════════╝

  USER
   │  withPlugin(Spark.basicPlugin())
   │
   ▼
╔══════════════════╗
║  WayangContext   ║
╚═════════┬════════╝
          │ registers
          ├──────────────────────────┬──────────────────────────┐
          ▼                          ▼                          ▼
   ┌─────────────┐          ┌──────────────────┐     ┌──────────────────┐
   │  Mappings   │          │ ChannelConversions│     │  Platform config │
   │(30+ rules)  │          │ (bridge rules)   │     │ (.properties)    │
   └──────┬──────┘          └────────┬─────────┘     └────────┬─────────┘
          │                          │                         │
          │                          │                         │
          ╔══════════════════════════════════════════════════════════════╗
          ║                    OPTIMIZATION PHASE                        ║
          ╠══════════════════════════════════════════════════════════════╣
          │                                                              │
          │  STAGE 1: prepareWayangPlan()                                │
          │  ┌───────────────────────────────────────────────────────┐   │
          │  │  WayangPlan (logical)                                 │   │
          │  │  FilterOp → ReduceByOp → CollectOp                   │   │
          │  │       + Mappings applied ──────────────────────────► │   │
          │  │  WayangPlan (hyperplan)                               │   │
          │  │  OperatorAlternative{ FilterOp                        │   │
          │  │                       SparkFilterOp                   │   │
          │  │                       PostgresFilterOp }              │   │
          │  └───────────────────────────────────────────────────────┘   │
          │                          │                                    │
          │  STAGE 2: estimateKeyFigures()                               │
          │  ┌───────────────────────────────────────────────────────┐   │
          │  │  CardinalityEstimatorManager                          │   │
          │  │    reads hyperplan                                    │   │
          │  │    per-operator: runs CardinalityEstimator            │   │
          │  │      JDBC: SELECT count(*) → exact count             │   │
          │  │      Others: formula / propagate from upstream       │   │
          │  │    writes estimated row counts into OptimizationCtx  │   │
          │  └───────────────────────────────────────────────────────┘   │
          │                          │                                    │
          │  STAGE 3: createInitialExecutionPlan()                        │
          │  ┌───────────────────────────────────────────────────────┐   │
          │  │  PlanEnumerator                                        │   │
          │  │    reads hyperplan + row counts                       │   │
          │  │    generates all possible physical plan combos        │   │
          │  │    e.g. [AllSpark] [AllJava] [Postgres+Spark] ...     │   │
          │  │                    │                                  │   │
          │  │  CostModel                                            │   │
          │  │    for each plan: row_count × load_formula → time    │   │
          │  │                   time × TimeToCostConverter → cost  │   │
          │  │    picks plan with lowest cost                        │   │
          │  │                    │                                  │   │
          │  │  ExecutionPlan.createFrom()                           │   │
          │  │    assigns Channels between operators                 │   │
          │  │    inserts auto-conversion ops (ChannelConversions)  │   │
          │  │    splits into ExecutionStages by platform boundary   │   │
          │  └───────────────────────────────────────────────────────┘   │
          ║                                                              ║
          ╚══════════════════════════════════════════════════════════════╝
                                     │
                                     │ ExecutionPlan (concrete, one platform per op)
                                     ▼
          ╔══════════════════════════════════════════════════════════════╗
          ║                    EXECUTION PHASE                           ║
          ╠══════════════════════════════════════════════════════════════╣
          │                                                              │
          │  CrossPlatformExecutor                                       │
          │    for each ExecutionStage:                                  │
          │      SparkExecutor ──► operator.evaluate(channels, compiler) │
          │      JdbcExecutor  ──► operator.createSqlClause()           │
          │                                                              │
          │  After each action:                                          │
          │    real cardinalities measured ──► CrossPlatformExecutor    │
          │                                          │                   │
          │                        ┌─────────────────┴──────────────┐   │
          │                        │  re-plan next stage?            │   │
          │                        │  YES → loop back to STAGE 2    │   │
          │                        │  NO  → continue execution       │   │
          │                        └────────────────────────────────┘   │
          │                                                              │
          │  After job completes:                                        │
          │    CardinalityRepository.storeAll() ──► JSON file on disk   │
          ║                                                              ║
          ╚══════════════════════════════════════════════════════════════╝
```

| Component | Role relative to optimizer | Phase |
|---|---|---|
| `Mappings` | **Input** — provides the substitution rules | Before optimization |
| `ChannelConversions` | **Input** — provides bridge operator rules | Before optimization |
| `WayangPlan` (hyperplan) | **Input** — the expanded graph to optimize over | Stage 1 |
| `CardinalityEstimatorManager` | **Feeds optimizer** — provides row count estimates | Stage 2 |
| `PlanEnumerator` | **Is part of optimizer** — generates all combos | Stage 3 |
| `CostModel` | **Is part of optimizer** — scores and picks best | Stage 3 |
| `ExecutionPlan` | **Output** — the chosen concrete physical plan | After Stage 3 |
| `CrossPlatformExecutor` | **Consumes output** — executes the plan | Execution phase |
| `CardinalityRepository` | **Feedback** — logs actuals for future manual tuning | After execution |

---

## 9. How to Add a New Platform (Summary)

To add a new platform (e.g. Trino), you need to implement:

| What | Example (Spark) | Purpose |
|---|---|---|
| `Platform` | `SparkPlatform` | Manages engine lifecycle, provides `ExecutorFactory` |
| `Executor` | `SparkExecutor` | Holds engine connection, calls `operator.evaluate()` |
| `Plugin` | `SparkBasicPlugin` | Registers mappings + channel conversions |
| `Mapping` (×N) | `FilterMapping`, `ReduceByMapping`, ... | Substitution rules for optimizer |
| `ExecutionOperator` (×N) | `SparkFilterOperator`, ... | Actual engine API calls |
| `Channel` | `RddChannel` | Data container between operators |
| `ChannelConversions` | `ChannelConversions.java` | Bridge to other platforms |
| `compiler/` (optional) | `FunctionCompiler` + adapters | Only needed if engine has its own function interfaces |
| `defaults.properties` | `wayang-spark-defaults.properties` | Cost formulas, core/memory config |
