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

## 8. How to Add a New Platform (Summary)

To add a new platform (e.g. Trino), you need to implement the following classes.
Each entry shows: stage, who uses it, its data members, references to other classes,
and core methods.

---

### `Plugin`
**Example:** `SparkBasicPlugin`
**Stage:** Setup — called once when user calls `withPlugin(...)`
**Used by:** `WayangContext`

**Data members:** none (stateless)

**References:**
- `Mappings.BASIC_MAPPINGS` — the full list of mapping rules for this platform
- `ChannelConversions.ALL` — the full list of channel conversion rules
- `SparkPlatform.getInstance()` — the platform singleton
- `JavaPlatform.getInstance()` — required because Spark output often feeds Java

**Core methods:**
```
getMappings()           → returns all Mapping rules for the optimizer
getChannelConversions() → returns all ChannelConversion rules for the optimizer
getRequiredPlatforms()  → declares which platforms must be registered
setProperties()         → (optional) set extra config values
```

---

### `Platform`
**Example:** `SparkPlatform`
**Stage:** Setup (loads config) + Execution (provides executor)
**Used by:** `CrossPlatformExecutor` and `WayangContext`

**Data members:**
```java
SparkContextReference sparkContextReference  // lazy-initialized, shared across jobs (JVM-wide singleton)
static SparkPlatform instance                // singleton
String PLATFORM_NAME = "Apache Spark"
String DEFAULT_CONFIG_FILE = "wayang-spark-defaults.properties"
String[] REQUIRED_SPARK_PROPERTIES          // e.g. "spark.master"
String[] OPTIONAL_SPARK_PROPERTIES          // e.g. "spark.executor.memory", "spark.executor.cores"
```

**References:**
- `SparkContextReference` — reference-counted wrapper around `JavaSparkContext`
- `SparkExecutor` — created by `getExecutorFactory()`
- `Configuration` — reads all properties from here

**Core methods:**
```
getInstance()                   → returns the singleton
getSparkContext(job)             → creates or reuses JavaSparkContext, applies config
getExecutorFactory()             → returns factory: job → new SparkExecutor(this, job)
configureDefaults(configuration) → loads wayang-spark-defaults.properties
createLoadProfileToTimeConverter → builds CPU/disk/network time estimator from config
createTimeToCostConverter        → builds cost-per-ms converter from config
warmUp(configuration)            → runs a trivial Spark job to pre-initialize SparkContext
```

---

### `Mapping` (one per operator)
**Example:** `FilterMapping`
**Stage:** Optimization — Stage 1 (`prepareWayangPlan`)
**Used by:** Optimizer via `gatherTransformations()` → `wayangPlan.applyTransformations()`

**Data members:** none (stateless — all logic is in the returned `PlanTransformation`)

**References:**
- `PlanTransformation` — the rule object that wraps pattern + factory + target platform
- `SubplanPattern` / `OperatorPattern` — describes what to look for in the plan
- `ReplacementSubplanFactory` — creates the physical operator when a match is found
- `SparkFilterOperator` — the physical operator this mapping produces
- `SparkPlatform` — tags the transformation as belonging to Spark

**Core methods:**
```
getTransformations()                → returns singleton PlanTransformation
createSubplanPattern()              → builds OperatorPattern matching FilterOperator (any predicate)
createReplacementSubplanFactory()   → builds factory: matchedOp → new SparkFilterOperator(matchedOp)
```

---

### `ExecutionOperator` (one per operator)
**Example:** `SparkFilterOperator`
**Stage:** Execution — called by `SparkExecutor.execute(task)`
**Used by:** `SparkExecutor` via `cast(task.getOperator()).evaluate(...)`

**Data members:**
```java
PredicateDescriptor<Type> predicateDescriptor  // holds the user's lambda
// (inherited from FilterOperator — logical operator parent)
```

**References:**
- `RddChannel` — declares supported input/output channel types
- `BroadcastChannel` — supported for broadcast input slot
- `FunctionCompiler` — used inside `evaluate()` to adapt the lambda
- `SparkExecutor` — received as parameter in `evaluate()`
- `LoadProfileEstimators` — provides cost estimation hooks

**Core methods:**
```
evaluate(inputs, outputs, sparkExecutor, operatorContext)
    → compiler.compile(predicateDescriptor)   // wrap lambda as Spark Function
    → inputs[0].provideRdd()                  // read input RDD from channel
    → inputRdd.filter(filterFunction)         // Spark call (lazy)
    → outputs[0].accept(outputRdd)            // write result RDD to output channel
    → return modelLazyExecution(...)          // signal: no Spark action triggered

getSupportedInputChannels(index)   → [RddChannel.UNCACHED, RddChannel.CACHED]
getSupportedOutputChannels(index)  → [RddChannel.UNCACHED]
containsAction()                   → false  (lazy operator)
getLoadProfileEstimatorConfigurationKey() → "wayang.spark.filter.load"
```

---

### `Executor`
**Example:** `SparkExecutor` (extends `PushExecutorTemplate`)
**Stage:** Execution — one instance per Spark stage
**Used by:** `CrossPlatformExecutor` — created via `SparkPlatform.getExecutorFactory()`

**Data members:**
```java
JavaSparkContext sc                  // live connection to Spark cluster
SparkSession ss                      // Spark SQL session
FunctionCompiler compiler            // adapts user lambdas to Spark functions
SparkPlatform platform               // reference back to the platform
SparkContextReference sparkContextReference  // reference-counted SC wrapper
int numDefaultPartitions             // computed from spark.executor.cores × machines
int numActions                       // counts how many Spark actions have fired
```

**References:**
- `SparkPlatform` — provides the `SparkContextReference`
- `FunctionCompiler` — owned here, passed into every `evaluate()` call via `this`
- `RddChannel` — forwarded between operators via `forward()`
- `ExecutionTask` — each task wraps one operator + its channel connections
- `PartialExecution` — records timing and cost after each action

**Core methods:**
```
execute(task, inputChannels, operatorContext, isRequestEager)
    → task.getOperator().createOutputChannelInstances(this, ...)  // create blank output channels
    → cast(task.getOperator()).evaluate(inputs, outputs, this, ...)// run the operator
    → createPartialExecution(lineageNodes, duration)              // record timing
    → registerMeasuredCardinalities(producedChannels)             // feedback to optimizer

getCompiler()           → returns FunctionCompiler
getNumDefaultPartitions() → returns numDefaultPartitions
forward(input, output)  → copies RDD reference from one RddChannel.Instance to another
dispose()               → releases SparkContext reference
```

---

### `Channel`
**Example:** `RddChannel` (extends `Channel`)
**Stage:** Optimization — created during `ExecutionPlan.createFrom()`; descriptor used at execution to create instances
**Used by:** Optimizer (wires plan); `SparkExecutor` (calls `createInstance()`)

**Data members (on `RddChannel`):**
```java
static ChannelDescriptor UNCACHED_DESCRIPTOR  // (reusable=false, cached=false)
static ChannelDescriptor CACHED_DESCRIPTOR    // (reusable=true, cached=true)
// (parent Channel holds: producer ExecutionTask, consumer ExecutionTask, DataSetType)
```

**References:**
- `Channel` (parent) — holds producer/consumer task references and data type
- `ChannelDescriptor` — metadata: is it reusable? is it cached?
- `RddChannel.Instance` — the runtime instance created from this channel blueprint

**Core methods:**
```
createInstance(executor, operatorContext, outputIndex)
    → new RddChannel.Instance(...)   // called by executor to create a live channel instance
copy()
    → new RddChannel(this)           // deep copy for plan branching
```

---

### `ChannelInstance`
**Example:** `RddChannel.Instance` (inner class of `RddChannel`, extends `AbstractChannelInstance`)
**Stage:** Execution — created fresh per task, disposed after stage completes
**Used by:** `SparkExecutor` (creates); `SparkFilterOperator` (reads/writes)

**Data members:**
```java
JavaRDD<?> rdd              // the actual RDD reference (points to distributed data on workers)
LongAccumulator accumulator // Spark accumulator for counting records (cardinality measurement)
```

**References:**
- `JavaRDD` — the actual distributed data reference (NOT the data itself)
- `LongAccumulator` — Spark's distributed counter, used when cardinality measurement is requested
- `SparkExecutor` — received in `accept()` to access `sc` for creating the accumulator

**Core methods:**
```
accept(rdd, sparkExecutor)
    → if instrumented: wraps rdd with a counting filter + accumulator
    → else: stores rdd directly
    // called by the producing operator to deposit its output

provideRdd()
    → returns this.rdd
    // called by the consuming operator to read the input

getMeasuredCardinality()
    → reads accumulator.value() from Spark workers → returns actual record count

doDispose()
    → reads final accumulator value → stores as measured cardinality
    → calls rdd.unpersist() if cached
```

---

### `ChannelConversions`
**Example:** `ChannelConversions.java` (Spark)
**Stage:** Optimization — used during `ExecutionPlan.createFrom()` at platform boundaries
**Used by:** Optimizer when it detects a channel type mismatch between adjacent operators

**Data members:** all `static final` constants, e.g.:
```java
UNCACHED_RDD_TO_CACHED_RDD    = RddChannel(uncached) → RddChannel(cached)   via SparkCacheOperator
COLLECTION_TO_UNCACHED_RDD    = CollectionChannel → RddChannel              via SparkCollectionSource
UNCACHED_RDD_TO_COLLECTION    = RddChannel → CollectionChannel              via SparkCollectOperator
CACHED_RDD_TO_HDFS_TSV        = RddChannel(cached) → FileChannel(HDFS)     via SparkTsvFileSink
HDFS_OBJECT_FILE_TO_UNCACHED_RDD = FileChannel → RddChannel                via SparkObjectFileSource
static Collection<ChannelConversion> ALL   // all of the above registered together
```

**References:**
- `DefaultChannelConversion` — each entry wraps: (sourceDescriptor, targetDescriptor, operator factory)
- `RddChannel`, `DatasetChannel`, `BroadcastChannel` — Spark-side channel types
- `CollectionChannel` — Java-side channel type (from `wayang-java`)
- `FileChannel` — HDFS file channel (from `wayang-basic`)

**Core methods:** none — this class is a static registry, no instance methods.

---

### `compiler/` (optional, Spark-specific)
**Example:** `FunctionCompiler` + adapters
**Stage:** Execution — called inside every `ExecutionOperator.evaluate()`
**Used by:** Each `SparkExecutionOperator` via `sparkExecutor.getCompiler().compile(...)`

**Data members:** none on `FunctionCompiler` (stateless)

**Adapter classes and what they hold:**
```
MapFunctionAdapter          wraps: java.util.function.Function<I,O>
FlatMapFunctionAdapter      wraps: java.util.function.Function<I,Iterable<O>>
PredicateAdapter            wraps: java.util.function.Predicate<T>
BinaryOperatorAdapter       wraps: java.util.function.BinaryOperator<T>
MapPartitionsFunctionAdapter wraps: java.util.function.Function<Iterable<I>,Iterable<O>>
KeyExtractor (inner)        wraps: java.util.function.Function<T,K>
```

Each adapter implements the corresponding Spark interface and delegates `.call()` → `.apply()`.

**Core methods on `FunctionCompiler`:**
```
compile(TransformationDescriptor, ...)   → MapFunctionAdapter or ExtendedMapFunctionAdapter
compile(PredicateDescriptor, ...)        → PredicateAdapter or ExtendedPredicateAdapter
compile(FlatMapDescriptor, ...)          → FlatMapFunctionAdapter or ExtendedFlatMapFunctionAdapter
compile(ReduceDescriptor, ...)           → BinaryOperatorAdapter or ExtendedBinaryOperatorAdapter
compile(MapPartitionsDescriptor, ...)    → MapPartitionsFunctionAdapter
compileToKeyExtractor(descriptor)        → KeyExtractor (PairFunction for mapToPair)
```

---

### `defaults.properties`
**Example:** `wayang-spark-defaults.properties`
**Stage:** Optimization — Stage 2 (`estimateKeyFigures`) and Stage 3 (`pickBestExecutionPlan`)
**Used by:** `CostModel`, `LoadProfileToTimeConverter`, `TimeToCostConverter`

**Key properties:**
```properties
wayang.spark.cpu.mhz           = 2700         # CPU speed per core
wayang.spark.machines          = 1            # number of machines in cluster
wayang.spark.cores-per-machine = 4            # cores per machine
wayang.spark.hdfs.ms-per-mb    = 2.7          # disk I/O speed
wayang.spark.network.ms-per-mb = 8.6          # network speed
wayang.spark.stretch           = 1.0          # safety margin multiplier

wayang.spark.costs.fix         = 0.0          # fixed cost per job
wayang.spark.costs.per-ms      = 1.0          # variable cost per millisecond

wayang.spark.init.ms           = 4000         # SparkContext startup cost

wayang.spark.filter.load       = {in} * 0.1  # CPU load formula for filter
wayang.spark.map.load          = {in} * 0.1  # CPU load formula for map
wayang.spark.reduceby.load     = ...          # CPU load formula for reduceByKey
```

These formulas are fed into `createLoadProfileToTimeConverter()` on `SparkPlatform` to
estimate how long each operator will take, which the cost model then converts to a cost
number for plan comparison.

---

### Summary Table

| Class | Stage | Used by | Key data members | Core responsibility |
|---|---|---|---|---|
| `Plugin` | Setup | `WayangContext` | none (stateless) | Hands mappings + channel conversions + platform to framework |
| `Platform` | Setup + Execution | `CrossPlatformExecutor` | `sparkContextReference`, `instance` | Manages engine lifecycle, provides `ExecutorFactory`, loads config |
| `Mapping` | Optimization stage 1 | Optimizer | none (stateless) | Substitution rule: logical op → physical op |
| `ExecutionOperator` | Execution | `Executor` | `predicateDescriptor` (user lambda) | Calls engine API, reads/writes `ChannelInstance`s |
| `Executor` | Execution | `CrossPlatformExecutor` | `sc`, `ss`, `compiler`, `numDefaultPartitions` | Loops tasks, creates channel instances, calls `evaluate()` |
| `Channel` | Optimization stage 3 | Optimizer + Executor | `UNCACHED_DESCRIPTOR`, `CACHED_DESCRIPTOR` | Blueprint of pipe: type + producer/consumer metadata |
| `ChannelInstance` | Execution | Executor + Operator | `rdd` (JavaRDD ref), `accumulator` | Live data container: `accept()` to fill, `provideRdd()` to read |
| `ChannelConversions` | Optimization stage 3 | Optimizer | static `ChannelConversion` constants | Registry of auto-bridge rules between channel types |
| `compiler/` | Execution | `ExecutionOperator` | none on compiler; adapters wrap Java lambdas | Wraps Java lambdas into engine-specific function interfaces |
| `defaults.properties` | Optimization stage 2+3 | `CostModel` | load formulas, CPU/disk/network specs | Drives cost estimation for plan comparison |

---

## 9. How the Optimizer Works — End-to-End Example

### Inputs

```
1. Hyperplan          — every operator has multiple platform alternatives
2. Cardinality        — estimated row count at each operator's output
3. Load formulas      — how expensive each operator is per row (from .properties)
4. Platform config    — startup costs, CPU speed, cores, etc.
```

### Minimal Cross-Platform Example

User writes:

```java
data.filter(x -> x.startsWith("A"))
    .reduceBy(x -> x.getKey(), (a, b) -> a + b)
    .collect()
```

Reading from a Postgres table with 10 million rows.

---

### Step 1 — Build the Hyperplan (Stage 1: `prepareWayangPlan`)

Mappings expand every logical operator into a menu of platform alternatives:

```
PostgresTableSource
    └── OperatorAlternative[filter]
          ├── JavaFilterOperator
          ├── SparkFilterOperator
          └── PostgresFilterOperator      ← has SQL: WHERE col LIKE 'A%'
    └── OperatorAlternative[reduceBy]
          ├── JavaReduceByOperator
          └── SparkReduceByOperator
    └── OperatorAlternative[collect]
          └── JavaCollectOperator / SparkCollectOperator
```

---

### Step 2 — Estimate Cardinalities (Stage 2: `estimateKeyFigures`)

For `PostgresTableSource`, runs `SELECT count(*)` → 10,000,000 rows.
Then propagates estimates through the plan:

```
PostgresTableSource   → 10,000,000 rows
      ↓ filter (selectivity ~10% assumed)
                      →  1,000,000 rows
      ↓ reduceBy (reduces to unique keys)
                      →     50,000 rows
```

These estimates are stored in `OptimizationContext` for every alternative.

---

### Step 3 — Enumerate and Score All Plans (Stage 3: `createInitialExecutionPlan`)

The cost formula for each operator is:

```
row_count × load_formula → LoadProfile (CPU/disk/network load)
    → LoadProfileToTimeConverter → TimeEstimate (milliseconds)
    → TimeToCostConverter        → cost = fixCost + costsPerMs × timeMs
```

Four representative plans:

```
Plan A — All Java:
  JavaFilter(10M rows) + JavaReduceBy(1M rows)
  → single-threaded, no startup
  → SLOW for 10M rows

Plan B — All Spark:
  SparkFilter(10M rows) + SparkReduceBy(1M rows)
  → parallel workers handle 10M well
  → + 4000ms SparkContext startup fixCost
  → MEDIUM

Plan C — Postgres filter + Spark reduceBy:        ← cross-platform winner
  PostgresFilter pushes WHERE to DB → only 1M rows leave DB
  SparkReduceBy(1M rows)
  + bridge operators (SqlToStream → SparkCollectionSource)
  + 4000ms Spark startup
  → CHEAPEST (DB does heavy filter, Spark sees only 1M rows)

Plan D — Postgres filter + Java reduceBy:
  PostgresFilter → 1M rows → JavaReduceBy(1M rows)
  → no Spark startup cost
  → MEDIUM (depends on row size fitting in Java memory)
```

Optimizer picks **Plan C**.

---

### Step 4 — Assemble the Concrete ExecutionPlan

```
Stage 1 (JdbcExecutor):
    PostgresTableSource → PostgresFilterOperator
        SQL: SELECT * FROM t WHERE col LIKE 'A%'
        out: SqlQueryChannel("SELECT ...")
              ↓
    SqlToStreamOperator    ← auto-inserted bridge (ChannelConversions)
        executes SQL, streams ResultSet rows
        out: CollectionChannel(List<Row>)

Stage 2 (SparkExecutor):
    SparkCollectionSource  ← auto-inserted bridge
        reads List<Row>, creates RDD
        out: RddChannel(JavaRDD<Row>)
              ↓
    SparkReduceByOperator
        rdd.mapToPair(key).reduceByKey(fn)
        out: RddChannel(JavaRDD<Result>)
              ↓
    SparkCollectOperator
        rdd.collect() → List<Result>
        out: CollectionChannel(List<Result>)

User receives: List<Result>
```

---

### Is the Optimizer Purely Cost-Based? Does it Consider Java Memory Limits?

**Short answer: yes, purely cost-based — and no, it does not model Java memory limits directly.**

The optimizer makes decisions entirely based on:

```
cost = f(row_count, load_formula, platform_config)
```

It does **not** have explicit rules like "Java can't handle more than X GB" or "this plan will OOM." What it does instead is model the **time cost** of processing large data on Java (slow, single-threaded) vs. Spark (fast, parallel). For large enough data the Java cost formula naturally becomes much higher than Spark, so the optimizer steers away from Java — not because it knows about memory, but because the time estimate is bad.

In practice this means:

```
If data is huge and Java is selected:  → will be slow or may OOM at runtime
                                          (optimizer doesn't predict OOM directly)

If load formulas are well-calibrated:  → Java cost grows fast enough with row count
                                          that Spark is always preferred above a threshold
```

This is a known limitation — the cost model is an approximation, not a memory safety guarantee. The load formulas in `.properties` files are hand-tuned estimates, not derived from actual memory profiling.

---

## 10. Plan Data Structures

There are two separate data structures representing the plan — one for each phase.

---

### `WayangPlan` — Logical / Hyperplan (Optimization Time)

Created by the user's API calls. After mappings are applied it becomes a
**hyperplan** — each node holds multiple platform alternatives simultaneously.

```
WayangPlan
  └── sinks: List<Operator>          ← entry points (traverse backwards to sources)

        Before mappings (logical plan):
          FilterOperator ──OutputSlot──► ReduceByOperator

        After mappings (hyperplan):
          OperatorAlternative[filter]
            ├── Alternative[0]: JavaFilterOperator
            ├── Alternative[1]: SparkFilterOperator
            └── Alternative[2]: PostgresFilterOperator
                  │
                  OutputSlot → InputSlot
                  │
          OperatorAlternative[reduceBy]
            ├── Alternative[0]: JavaReduceByOperator
            └── Alternative[1]: SparkReduceByOperator
```

Operators are connected via typed **slots**:
- `OutputSlot` on the producing operator
- `InputSlot` on the consuming operator

The hyperplan is a **graph of `OperatorAlternative` nodes** — it represents
all possible physical plans at once. The optimizer reads this to enumerate
combinations.

---

### `ExecutionPlan` — Physical Plan (After Optimizer Picks Winners)

Produced by the optimizer. One concrete operator per node, channels wired,
split into stages by platform boundary.

```
ExecutionPlan
  └── ExecutionStage[]               ← one stage per platform boundary
        │
        ├── Stage 1  (runs on JdbcExecutor)
        │     └── ExecutionTask[]    ← one task per operator
        │           ├── ExecutionTask
        │           │     ├── operator:  PostgresTableSource
        │           │     ├── inputs:    Channel[]  (empty for source)
        │           │     └── outputs:   Channel[]  (SqlQueryChannel)
        │           │
        │           └── ExecutionTask
        │                 ├── operator:  PostgresFilterOperator
        │                 ├── inputs:    Channel[]  (SqlQueryChannel)
        │                 └── outputs:   Channel[]  (SqlQueryChannel)
        │
        └── Stage 2  (runs on SparkExecutor)
              └── ExecutionTask[]
                    ├── ExecutionTask
                    │     ├── operator:  SparkCollectionSource  ← auto-inserted bridge
                    │     ├── inputs:    Channel[]  (CollectionChannel)
                    │     └── outputs:   Channel[]  (RddChannel)
                    │
                    ├── ExecutionTask
                    │     ├── operator:  SparkReduceByOperator
                    │     ├── inputs:    Channel[]  (RddChannel)
                    │     └── outputs:   Channel[]  (RddChannel)
                    │
                    └── ExecutionTask
                          ├── operator:  SparkCollectOperator
                          ├── inputs:    Channel[]  (RddChannel)
                          └── outputs:   Channel[]  (CollectionChannel)
```

Each `ExecutionTask` wraps exactly **one** chosen `ExecutionOperator` and its
wired `Channel`s. No more alternatives — one concrete operator per node.

---

### How They Relate

```
WayangPlan (hyperplan)
  OperatorAlternative{ Java | Spark | Postgres }   ← menu of choices
          │
          │  optimizer enumerates all combos
          │  scores each with cost formula
          │  picks one alternative per node
          ▼
ExecutionPlan
  ExecutionTask{ PostgresFilterOperator }
      ──[SqlQueryChannel]──
  ExecutionTask{ SparkReduceByOperator }            ← chosen meal
```

| | `WayangPlan` | `ExecutionPlan` |
|---|---|---|
| **When created** | User API calls | Optimizer output |
| **Node type** | `OperatorAlternative` (many choices) | `ExecutionTask` (one operator) |
| **Connections** | `OutputSlot → InputSlot` | `Channel` objects |
| **Platform info** | None (platform-agnostic) | One platform per task |
| **Used by** | Optimizer (reads to enumerate) | Executor (runs task by task) |
| **Lifespan** | Entire job | Entire job (updated on re-optimization) |

---

## 11. What is `ExecutionStage`?

`ExecutionStage` is **a pure data/metadata object** — it holds no execution state and runs nothing itself. The Javadoc explicitly says: *"this class is immutable, i.e., it does not comprise any execution state."*

It is the **minimum unit of work** that `CrossPlatformExecutor` can schedule, pause, or hand off to a platform executor.

### Object Hierarchy

```
ExecutionPlan
  └── PlatformExecution             ← groups all stages for one platform (e.g., "Spark")
        ├── platform: Platform      ← which engine owns these stages
        └── stages: List<ExecutionStage>
              └── ExecutionStage
                    ├── platformExecution: PlatformExecution   ← back-ref to parent
                    ├── startTasks: List<ExecutionTask>        ← entry points (inputs from other stages)
                    ├── terminalTasks: List<ExecutionTask>     ← exit points (outputs cross stage boundary)
                    ├── predecessors: List<ExecutionStage>     ← must finish before this runs
                    ├── successors: Set<ExecutionStage>        ← activated after this completes
                    ├── executionStageLoop: ExecutionStageLoop ← non-null if inside an iterative loop
                    └── sequenceNumber: int                    ← for debugging/ordering
```

### Data Fields

| Field | Type | Purpose |
|---|---|---|
| `platformExecution` | `PlatformExecution` | Which platform owns this stage (Spark, JDBC, etc.) |
| `startTasks` | `Collection<ExecutionTask>` | Entry-point tasks — their inputs come from cross-stage channels |
| `terminalTasks` | `Collection<ExecutionTask>` | Exit-point tasks — their outputs go into cross-stage channels |
| `predecessors` | `Collection<ExecutionStage>` | Stages that must complete before this one can activate |
| `successors` | `Set<ExecutionStage>` | Stages that get activated once this one completes |
| `executionStageLoop` | `ExecutionStageLoop` | Non-null if this stage is part of an iterative loop |

### Key Operations

| Method | What it does |
|---|---|
| `getAllTasks()` | BFS from `startTasks` → returns every `ExecutionTask` in this stage |
| `getOutboundChannels()` | Channels that cross into successor stages (this stage's outputs) |
| `getInboundChannels()` | Channels that come from predecessor stages (this stage's inputs) |
| `addSuccessor(stage)` | Wires two stages together (sets predecessor/successor links mutually) |
| `isLoopHead()` | True if this stage is the entry point of an iterative loop |
| `isInFinishedLoop()` | True if the loop this stage belongs to has completed all iterations |
| `getPlanAsString()` | Prints all tasks and channels in this stage (for logging/debugging) |

### Why Stages Exist (Not Just Tasks)

One `PlatformExecution` (e.g., a Spark job) can have **multiple stages** because:

1. **Re-optimization breakpoints** — Wayang can pause between stages, update cardinality estimates from runtime measurements, and re-plan the remainder of the job.
2. **Cross-platform data handoff** — Stage 2 (Spark) can only start once Stage 1 (JDBC) has finished producing its output channels. The stage boundary is where this handoff is made explicit.
3. **Iterative loops** — Algorithms like gradient descent or PageRank iterate. Each iteration is a stage; `ExecutionStageLoop` tracks the iteration count and termination condition.

---

## 12. How the Executor Walks the ExecutionPlan

Execution is split across two levels:
- **`CrossPlatformExecutor`** — orchestrates across all stages and platforms
- **Platform executor** (e.g., `SparkExecutor`, `JdbcExecutor`) — runs tasks within a single stage

### Level 1: `CrossPlatformExecutor` — schedules stages

```
CrossPlatformExecutor.execute(executionPlan)
    │
    ├── loads starting ExecutionStages into activatedStageActivators queue
    │     (stages whose predecessors are all done, or have no predecessors)
    │
    └── loop: while queue not empty
          │
          ├── poll next ready stage (StageActivator)
          │
          ├── getOrCreateExecutorFor(stage)
          │     → looks up stage.getPlatformExecution().getPlatform()
          │     → if no executor yet: platform.getExecutorFactory().create(job)
          │           → new SparkExecutor(...)   for Spark stages
          │           → new JdbcExecutor(...)    for JDBC stages
          │
          ├── executor.execute(stage, optimizationContext, this)
          │     → hands the entire stage to the platform executor
          │
          ├── tryToActivateSuccessors(stage)
          │     → for each outbound channel: check if next stage's inputs are all filled
          │     → if yes: add successor stage to the queue
          │
          └── dispose finished ChannelInstances (free memory)
```

Stages activate like dominoes: Stage 2 only enters the queue once Stage 1's output `ChannelInstance`s are filled.

### Level 2: `SparkExecutor` — runs tasks inside a stage

```
SparkExecutor.execute(stage)
    │
    └── for each ExecutionTask in stage (topological order):
          │
          ├── task.getOperator().createOutputChannelInstances(executor, task, ...)
          │     → creates blank RddChannel.Instance / CollectionChannel.Instance etc.
          │
          ├── cast(task.getOperator()).evaluate(
          │       inputChannelInstances,    ← filled by previous task (or inbound from prior stage)
          │       outputChannelInstances,   ← blank, to be filled by this task
          │       this,                     ← SparkExecutor (carries compiler + SparkContext)
          │       operatorContext
          │   )
          │     → operator reads input channel, calls Spark API, writes output channel
          │
          ├── if containsAction():
          │     → an actual Spark action (e.g., rdd.collect()) fires the DAG
          │
          └── registerMeasuredCardinalities()
                → reads Spark accumulators → stores measured sizes → feedback to optimizer
```

### End-to-End Walk: Postgres Filter → Spark ReduceBy

```
CrossPlatformExecutor starts
    │
    ├── Stage 1 activated (no predecessors — it's a source stage)
    │     JdbcExecutor.execute(Stage 1)
    │       │
    │       ├── task: PostgresTableSource
    │       │     evaluate() → assembles SQL "SELECT * FROM t"
    │       │     output: SqlQueryChannel.Instance("SELECT * FROM t")
    │       │
    │       └── task: PostgresFilterOperator
    │             evaluate() → appends WHERE clause
    │             output: SqlQueryChannel.Instance("SELECT * FROM t WHERE col LIKE 'A%'")
    │
    │     Stage 1 complete → outbound SqlQueryChannel filled
    │     tryToActivateSuccessors() → Stage 2 inputs are ready → Stage 2 activated
    │
    └── Stage 2 activated
          SparkExecutor.execute(Stage 2)
            │
            ├── task: SqlToStreamOperator           ← bridge operator
            │     evaluate() → runs SQL on Postgres via JDBC
            │                → streams ResultSet → List<Row>
            │     output: CollectionChannel.Instance(List<Row>)
            │
            ├── task: SparkCollectionSource          ← bridge operator
            │     evaluate() → sc.parallelize(list) → JavaRDD<Row>
            │     output: RddChannel.Instance(JavaRDD<Row>)    ← LAZY (no data yet)
            │
            ├── task: SparkReduceByOperator
            │     evaluate() → compiler.compile(reduceDesc) → Spark Function2
            │                → rdd.mapToPair(key).reduceByKey(fn)
            │     output: RddChannel.Instance(JavaRDD<Result>) ← LAZY (no data yet)
            │
            └── task: SparkCollectOperator
                  evaluate() → rdd.collect()   ← ACTION: entire Spark DAG fires NOW
                  output: CollectionChannel.Instance(List<Result>)

User receives: List<Result>
```

### Component Role Summary

```
CrossPlatformExecutor    →  walks ExecutionPlan stage by stage
                            activates successor stages when predecessor channels are filled
                            creates the right Executor per platform (one per Platform type)

SparkExecutor / JdbcExecutor  →  walks ExecutionTask list inside one stage
                                  creates blank output ChannelInstances
                                  calls operator.evaluate() task by task
                                  passes filled output as next task's input

operator.evaluate()      →  reads input ChannelInstance
                            calls Compiler to wrap user lambda into Spark Function
                            calls engine API (rdd.filter, rdd.reduceByKey, JDBC, etc.)
                            writes result into output ChannelInstance

ExecutionStage           →  pure metadata: holds task list + predecessor/successor links
                            the minimum schedulable unit; contains no execution state itself
```

The `ExecutionPlan` is the complete script. Executors just follow it — they make no decisions. All decisions were made by the optimizer before execution begins.
### Step 1 — Understand the existing architecture

Before adding anything, map out how the existing system works end-to-end. There are three distinct zones: **Optimizer**, **Plan**, and **Executor**. A new platform must plug into all three.

```
╔══════════════════════════════════════════════════════════════════════════════╗
║                          WAYANG ARCHITECTURE                                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  USER API                                                                    ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  wayangContext.with(SparkPlugin).with(JavaPlugin)                            ║
║  planBuilder.readFrom(source).filter(fn).reduceByKey(fn).collect()           ║
║       │                                                                      ║
║       │  builds                                                              ║
║       ▼                                                                      ║
║  ┌─────────────────────────────────────────────────────┐                    ║
║  │                    WayangPlan                        │                    ║
║  │  (logical, platform-agnostic)                        │                    ║
║  │                                                      │                    ║
║  │  FilterOperator → ReduceByOperator → CollectOp       │                    ║
║  │  (each wrapped in OperatorAlternative after mapping) │                    ║
║  └─────────────────────────────────────────────────────┘                    ║
║       │                                                                      ║
║       │                                                                      ║
╠═══════╪══════════════════════════════════════════════════════════════════════╣
║       │           OPTIMIZER ZONE                                             ║
║       │                                                                      ║
║       │  1. applyTransformations()                                           ║
║       │     Mappings registered by each Plugin turn logical operators        ║
║       │     into OperatorAlternatives (a menu of platform choices):          ║
║       │                                                                      ║
║       │     FilterOperator                                                   ║
║       │       ├── SparkFilterOperator   (registered by SparkPlugin)          ║
║       │       ├── JavaFilterOperator    (registered by JavaPlugin)           ║
║       │       └── TrinoFilterOperator   (registered by TrinoPlugin) ← NEW   ║
║       │                                                                      ║
║       │  2. estimateKeyFigures()                                             ║
║       │     CardinalityEstimator fills in estimated row counts per slot.     ║
║       │                                                                      ║
║       │  3. enumerate() + pickBestPlan()                                     ║
║       │     PlanEnumerator tries every combo of alternatives.                ║
║       │     Scores each combo: cost = Σ loadFormula(operator, cardinality)   ║
║       │     Picks the lowest-cost complete assignment.                       ║
║       │                                                                      ║
║       ▼                                                                      ║
║  ┌─────────────────────────────────────────────────────┐                    ║
║  │                   ExecutionPlan                      │                    ║
║  │  (physical, one platform per task)                   │                    ║
║  │                                                      │                    ║
║  │  PlatformExecution[Trino]                            │                    ║
║  │    ExecutionStage                                    │                    ║
║  │      ExecutionTask{ TrinoTableScanOperator }         │                    ║
║  │        ──[TrinoQueryChannel]──                       │                    ║
║  │      ExecutionTask{ TrinoFilterOperator }            │                    ║
║  │        ──[TrinoQueryChannel]──                       │                    ║
║  │      ExecutionTask{ TrinoCollectOperator }           │                    ║
║  │        ──[TrinoResultChannel]──                      │                    ║
║  │  PlatformExecution[Spark]                            │                    ║
║  │    ExecutionStage                                    │                    ║
║  │      ExecutionTask{ SparkReduceByOperator }          │                    ║
║  └─────────────────────────────────────────────────────┘                    ║
║                                                                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                          EXECUTOR ZONE                                       ║
║                                                                              ║
║  CrossPlatformExecutor                                                       ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  walks ExecutionPlan stage by stage (topological order)                      ║
║                                                                              ║
║  for each ExecutionStage:                                                    ║
║    getOrCreateExecutorFor(stage.getPlatform())                               ║
║      → TrinoExecutor   for Trino stages   ← NEW                             ║
║      → SparkExecutor   for Spark stages                                      ║
║      → JavaExecutor    for Java stages                                       ║
║                                                                              ║
║    executor.execute(stage)                                                   ║
║      for each ExecutionTask in stage:                                        ║
║        create blank output ChannelInstances                                  ║
║        operator.evaluate(inputs, outputs, executor, context)                 ║
║          → TrinoFilterOperator wraps SQL string                              ║
║          → TrinoCollectOperator submits SQL → Trino REST API → rows          ║
║        registerMeasuredCardinalities() → feedback to optimizer               ║
║                                                                              ║
║    tryToActivateSuccessors()                                                 ║
║      → next stage runs once output ChannelInstances are filled               ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
```


**How the existing JDBC connector solves this**: the user provides both implementations manually via `withSqlImplementation()`:

```java
// User writes the same logic twice — once in Java, once in SQL
new PredicateDescriptor<>(
    record -> record.getDouble("amount") > 100,   // Java lambda — for Spark/Java
    Record.class
).withSqlImplementation("amount > 100");          // SQL string — for JDBC/Trino
                                                  // user types this by hand
```

`PredicateDescriptor` stores both. Each platform reads only its own version:

```
Optimizer picks Trino/JDBC:
  operator.createSqlClause() → descriptor.getSqlImplementation() → "amount > 100"
  (Java lambda never touched)

Optimizer picks Spark/Java:
  compiler.compile(descriptor) → wraps Java lambda as Spark Function
  (SQL string never touched)
```

**How the JDBC executor chains multiple operators into one SQL**: it does NOT use the progressive subquery wrapping approach. Instead, `JdbcExecutor.createSqlString()` walks the entire stage, collects each operator's SQL clause, and assembles one flat SQL string centrally:

```java
// JdbcExecutor.createSqlString() — real code
sb.append("SELECT ").append(projection)    // from JdbcProjectionOperator
  .append(" FROM ").append(tableName)      // from JdbcTableSource
  .append(join)                            // from JdbcJoinOperator
  .append(" WHERE ").append(conditions);   // from all JdbcFilterOperators AND-ed together
```

Result for `TableScan → Filter(age>18) → Project(name)`:
```sql
SELECT name FROM customer WHERE age >= 18;
```

**Why JDBC can't support Aggregate this way**: the central assembler only knows how to build `SELECT ... FROM ... WHERE ... JOIN ...`. Adding `GROUP BY` changes the query shape and the assembler doesn't support it. This is why JDBC template is limited to Filter, Project, Join only.

**Our Trino approach uses per-operator subquery wrapping instead**:

```
TrinoFilter.evaluate():   "SELECT * FROM (...) WHERE amount > 100"
TrinoAggregate.evaluate(): "SELECT region, SUM(amount) FROM (...) GROUP BY region"
```

Each operator independently wraps the previous SQL — no central assembler, no shape constraints. Any SQL construct works.

**What operators can be SQL-eligible**:

```
filter(fn)      → WHERE clause    ✓  if fn has .withSqlImplementation()
project(cols)   → SELECT cols     ✓  always (just column names)
groupBy + agg   → GROUP BY        ✓  if agg function is standard (SUM/COUNT/MAX)
join            → JOIN            ✓  if join key is a column name

map(arbitrary)  → ???             ✗  no SQL equivalent for arbitrary transformations
reduceByKey(fn) → ???             ✗  only if fn is SUM/COUNT/MAX, not custom lambdas
stateful ops    → ???             ✗  no SQL equivalent
```

**Operator eligibility** (applies to all three approaches below):

```
filter(fn)      → WHERE clause    ✓  if fn has .withSqlImplementation()
project(cols)   → SELECT cols     ✓  always (just column names)
groupBy + agg   → GROUP BY        ✓  if agg function is standard (SUM/COUNT/MAX)
join            → JOIN            ✓  if join key is a column name

map(arbitrary)  → ???             ✗  no SQL equivalent for arbitrary transformations
reduceByKey(fn) → ???             ✗  only if fn is SUM/COUNT/MAX, not custom lambdas
stateful ops    → ???             ✗  no SQL equivalent
```

If no Trino mapping exists for an operator, the optimizer automatically falls back to Spark/Java. No explicit fallback code needed.

---



### Integration Options

Wayang has two connector styles. The choice determines the entire implementation shape:

| | JDBC Template | Custom Executor |
|---|---|---|
| **Examples** | Postgres, MySQL | Spark, Flink |
| **How it works** | Shared SQL builder handles all operators | Each operator owns its `evaluate()` logic |
| **Operator support** | Filter, Project, Join only | Any operation the engine supports |
| **Result format** | Row-by-row JDBC fetch | Engine-native (Arrow, columnar, RDD, etc.) |
| **Engine-specific tuning** | Not possible | Full control (session props, hints, etc.) |

**Data lake engines → Custom Executor.** These engines are OLAP-oriented and designed for full SQL pushdown, distributed execution, and columnar data formats. The JDBC template's limited operator set and row-by-row fetching would leave most of their performance on the table.