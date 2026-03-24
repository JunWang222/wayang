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

# Spark Connector — Architecture Deep Dive

This document explains how the Spark connector works end-to-end, from user API
call through to actual distributed execution on a Spark cluster. It covers all
four key packages (`channels`, `mapping`, `compiler`, `operators`) and includes
sequence diagrams for each execution phase with code references.

---

## 1. The Fundamental Difference vs. JDBC

Before diving in, the most important thing to understand is how Spark operators
work differently from JDBC operators.

```
JDBC operators:                        Spark operators:
───────────────                        ───────────────
createSqlClause()                      evaluate(inputs, outputs, sparkExecutor)
→ returns a String                     → calls rdd.filter() / rdd.reduceByKey()
→ JdbcExecutor assembles SQL           → returns an RddChannel holding the new RDD
→ sends to DB server                   → Spark DAG grows lazily
→ DB does the work                     → actual execution deferred until action
```

JDBC operators contribute SQL fragments. Spark operators **build a lazy RDD DAG**
that only runs when a Spark action (like `collect()`) is triggered.

---

## 2. Package Overview: Four Core Concepts

The Spark connector is organized into four packages, each with a distinct role.
Think of it like plumbing:

```
[Source] ──── channel ──── [Operator] ──── channel ──── [Operator] ──── channel ──── [Sink]
               pipe                          pipe
```

| Package | Role | Data or Process? | When does it run? |
|---|---|---|---|
| `channels/` | The **pipes** — typed containers that carry data between operators | Data-focused | Always |
| `operators/` | The **valves** — units of computation that transform data | Process-focused | Execution time |
| `mapping/` | The **rulebook** — tells the optimizer which operator class to use | Plan-focused | Optimization time |
| `compiler/` | The **adapter kit** — converts user lambdas into Spark-callable functions | Glue | Execution time (inside operators) |

---

## 3. Package Deep Dive

### 3.1 `channels/` — The Pipes (Data-focused)

A channel is **a typed container that carries data between two operators**. It
does not process data; it just holds a reference to it.

For Spark, the main channels are:

| Channel | What it holds |
|---|---|
| `RddChannel` | A `JavaRDD<T>` reference — the most common Spark channel |
| `DatasetChannel` | A Spark `Dataset<T>` (for structured/SQL-style data) |
| `BroadcastChannel` | A Spark broadcast variable (replicated to all workers) |
| `FileChannels` | A path to a file on HDFS |

`RddChannel` comes in two flavors — uncached (default) and cached (for RDDs
that are reused across multiple operators, i.e. `rdd.cache()`):

```java
// RddChannel.java
public static final ChannelDescriptor UNCACHED_DESCRIPTOR = new ChannelDescriptor(
        RddChannel.class, false, false
);
public static final ChannelDescriptor CACHED_DESCRIPTOR = new ChannelDescriptor(
        RddChannel.class, true, true
);
```

**`ChannelConversions.java`** defines how to automatically convert one channel
type into another. The optimizer uses this to insert conversion operators
automatically when needed:

```
RddChannel ──[SparkCollectOperator]──► CollectionChannel   (RDD → Java List)
CollectionChannel ──[SparkCollectionSource]──► RddChannel  (Java List → RDD)
RddChannel ──[SparkCacheOperator]──► RddChannel(cached)    (uncached → cached)
RddChannel ──[SparkTsvFileSink]──► FileChannel(HDFS)       (RDD → HDFS file)
```

For example, if a Spark pipeline feeds into a Java Streams pipeline, the
optimizer automatically inserts a `SparkCollectOperator` to bridge the two:

```java
// ChannelConversions.java
public static final ChannelConversion UNCACHED_RDD_TO_COLLECTION = new DefaultChannelConversion(
        RddChannel.UNCACHED_DESCRIPTOR,
        CollectionChannel.DESCRIPTOR,
        () -> new SparkCollectOperator<>(...)   // ← auto-inserted by optimizer
);
```

---

### 3.2 `mapping/` — The Optimizer's Rulebook (Plan-focused)

A mapping answers one question: **"When the optimizer sees logical operator X,
which physical Spark operator should replace it?"**

Mappings run **only during optimization**, never during execution. Each mapping
file = one substitution rule.

```java
// FilterMapping.java
// Rule: find any FilterOperator in the logical plan → replace with SparkFilterOperator
public Collection<PlanTransformation> getTransformations() {
    return Collections.singleton(new PlanTransformation(
            this.createSubplanPattern(),           // look for FilterOperator
            this.createReplacementSubplanFactory(), // replace with SparkFilterOperator
            SparkPlatform.getInstance()            // tag as Spark-specific
    ));
}

private ReplacementSubplanFactory createReplacementSubplanFactory() {
    return new ReplacementSubplanFactory.OfSingleOperators<FilterOperator>(
            (matchedOperator, epoch) -> new SparkFilterOperator<>(matchedOperator).at(epoch)
    );
}
```

**Key difference from JDBC mappings:** Spark mappings have no gate. Any
predicate — including pure Java lambdas — can be pushed to Spark. JDBC mappings
only accept operators that have a SQL string implementation:

```java
// PostgresFilterMapping — only maps if a SQL string exists
.withAdditionalTest(op -> op.getPredicateDescriptor().getSqlImplementation() != null)

// SparkFilterMapping — no gate, any lambda works
// (any FilterOperator can run on Spark)
```

There are 30+ mappings in `mapping/`:

```
FilterMapping, MapMapping, FlatMapMapping, ReduceByMapping, GlobalReduceMapping,
JoinMapping, CartesianMapping, CoGroupMapping, SortMapping, DistinctMapping,
CountMapping, MaterializedGroupByMapping, GlobalMaterializedGroupMapping,
TextFileSourceMapping, TextFileSinkMapping, ObjectFileSourceMapping, ObjectFileSinkMapping,
CollectionSourceMapping, LocalCallbackSinkMapping, LoopMapping, DoWhileMapping,
RepeatMapping, ZipWithIdMapping, UnionAllMapping, IntersectMapping,
SampleMapping, MapPartitionsMapping, ParquetSourceMapping, ParquetSinkMapping,
KafkaTopicSourceMapping, KafkaTopicSinkMapping, ...
```

---

### 3.3 `compiler/` — The Lambda Adapter (Glue)

**The problem:** Spark requires its own specific function interfaces like
`org.apache.spark.api.java.function.Function<I,O>`. But Wayang stores user
lambdas as standard Java interfaces like `java.util.function.Function<I,O>`.
These are different types — Spark won't accept a plain Java function.

**The solution:** `FunctionCompiler` picks the right adapter class and wraps
the user's lambda inside a Spark-compatible interface.

```java
// FunctionCompiler.java — compile() for a predicate (e.g. used by SparkFilterOperator)
public <Type> Function<Type, Boolean> compile(
        PredicateDescriptor<Type> predicateDescriptor, ...) {
    final Predicate<Type> javaImplementation = predicateDescriptor.getJavaImplementation();
    if (javaImplementation instanceof PredicateDescriptor.ExtendedSerializablePredicate) {
        return new ExtendedPredicateAdapater<>(javaImplementation, new SparkExecutionContext(...));
    } else {
        return new PredicateAdapter<>(javaImplementation);  // simple case
    }
}
```

The adapter itself is trivially simple — it just delegates the call:

```java
// MapFunctionAdapter.java — wraps java.util.function.Function as spark.api.java.function.Function
public class MapFunctionAdapter<I, O> implements Function<I, O> {  // ← Spark's Function
    private java.util.function.Function<I, O> function;             // ← user's Java lambda

    @Override
    public O call(I dataQuantum) throws Exception {
        return this.function.apply(dataQuantum);  // just delegates
    }
}
```

Each adapter class corresponds to a different Spark function interface:

| Adapter | Wayang descriptor | Spark interface produced |
|---|---|---|
| `MapFunctionAdapter` | `TransformationDescriptor` | `Function<I,O>` — for `rdd.map()` |
| `FlatMapFunctionAdapter` | `FlatMapDescriptor` | `FlatMapFunction<I,O>` — for `rdd.flatMap()` |
| `PredicateAdapter` | `PredicateDescriptor` | `Function<T,Boolean>` — for `rdd.filter()` |
| `BinaryOperatorAdapter` | `ReduceDescriptor` | `Function2<T,T,T>` — for `rdd.reduceByKey()` |
| `MapPartitionsFunctionAdapter` | `MapPartitionsDescriptor` | `FlatMapFunction<Iterator<I>,O>` — for `rdd.mapPartitions()` |
| `KeyExtractor` (inner class) | `TransformationDescriptor` | `PairFunction<T,K,T>` — for `rdd.mapToPair()` |

The `Extended*` variants handle a special case: functions that need access to
Spark execution context (broadcast variables, loop iteration number) are wrapped
with `SparkExecutionContext`.

---

### 3.4 `operators/` — The Valves (Process-focused)

Each operator is **a unit of computation**. Its job is:
1. Pull data from input channels
2. Use the compiler to adapt the user's lambda into a Spark function
3. Apply a Spark transformation (lazy) or action (eager)
4. Push the result into output channels

```
Operator's evaluate() = compile lambda + call Spark API + pass channel
```

There are three kinds of operators:

| Kind | Examples | What it does |
|---|---|---|
| **Source** | `SparkTextFileSource`, `SparkCollectionSource` | Creates a new RDD from external data |
| **Transformation** | `SparkFilterOperator`, `SparkReduceByOperator`, `SparkMapOperator` | Takes RDD in, outputs new RDD (lazy) |
| **Sink / Action** | `SparkCollectOperator`, `SparkTextFileSink` | Triggers real Spark execution |

`SparkExecutionOperator` is just the **marker interface** all of them implement —
it tells Wayang "this operator can run on Spark."

**Dependency between the three packages:**

```
Mapping  ──creates──►  Operator  ──uses──►  Compiler
   │                      │
   │ (optimization time)  │ (execution time)
   │                      │
   ▼                      ▼
rewrites the plan     calls rdd.filter() / rdd.reduceByKey() etc.
```

- **Mapping and Compiler** don't know about each other.
- **Operator** is the bridge — created by Mapping, uses Compiler.

---

## 4. Core Classes

| Class | File | Responsibility |
|---|---|---|
| `SparkPlatform` | `platform/SparkPlatform.java` | Manages `SparkContext` lifecycle, provides `getExecutorFactory()` |
| `SparkExecutor` | `execution/SparkExecutor.java` | Holds `JavaSparkContext` + `SparkSession`. Calls `evaluate()` on each operator one by one. |
| `FunctionCompiler` | `compiler/FunctionCompiler.java` | Wraps Wayang `FunctionDescriptor`s into Spark `Function<T,R>` objects |
| `SparkExecutionOperator` | `operators/SparkExecutionOperator.java` | Marker interface. All Spark operators implement this. |
| `SparkTextFileSource` | `operators/SparkTextFileSource.java` | Reads text file via `sc.textFile()` → produces `RddChannel` |
| `SparkFilterOperator` | `operators/SparkFilterOperator.java` | Compiles predicate, calls `rdd.filter(fn)` → produces filtered `RddChannel` |
| `SparkReduceByOperator` | `operators/SparkReduceByOperator.java` | Compiles key+reduce, calls `rdd.mapToPair().reduceByKey()` → produces reduced `RddChannel` |
| `SparkCollectOperator` | `operators/SparkCollectOperator.java` | Calls `rdd.collect()` — the **action** that triggers real Spark execution |
| `RddChannel` | `channels/RddChannel.java` | Channel that carries a `JavaRDD<T>` reference between Spark operators |

---

## 5. End-to-End Sequence Diagrams

### Phase 1 — Setup (`withPlugin`)

What happens when the user registers the Spark plugin.

```
User                    SparkPlugin              WayangContext         SparkPlatform
────                    ───────────              ─────────────         ─────────────
new WayangContext()
  .withPlugin(Spark.basicPlugin())
        │
        └──► SparkPlugin
               getRequiredPlatforms() ──────────────────────────────► registers SparkPlatform
               getMappings()          ──────────────────────────────► registers 30+ mappings
               getChannelConversions()──────────────────────────────► registers RddChannel conversions
               setProperties()
                    └──► SparkPlatform.configureDefaults()
                               └── loads wayang-spark-defaults.properties
                                   (spark.master, cores, memory, cost formulas)
```

---

### Phase 2 — Optimization

Mappings are applied here. Compiler and operators are NOT involved yet.

```
User           Optimizer            SparkFilterMapping      SparkReduceByMapping
────           ─────────            ──────────────────      ────────────────────

.collect()
    │
    └──► optimize(logicalPlan)
              │
              ├── apply SparkFilterMapping
              │       SubplanPattern: find FilterOperator (any predicate)
              │       → replace with SparkFilterOperator
              │
              ├── apply SparkReduceByMapping
              │       SubplanPattern: find ReduceByOperator
              │       → replace with SparkReduceByOperator
              │
              ├── estimate cost:
              │       filter:   wayang.spark.filter.load formula
              │       reduceby: wayang.spark.reduceby.load formula
              │       platform: high fixed startup (SparkContext ~seconds)
              │
              └── compare plans:
                    Plan A: all on Spark    cost = X (high fixed, scales well)
                    Plan B: all on Java     cost = Y (low startup, doesn't scale)
                    → picks based on estimated data volume
```

The optimizer weighs the high SparkContext startup cost against the benefit of
distributed execution. For small datasets Java Streams wins; for large datasets
Spark wins.

---

### Phase 3 — Execution (operator by operator, lazy)

`SparkExecutor` calls `evaluate()` on each operator one at a time. Each call
extends the Spark DAG but **does not execute anything yet**. This is where
operators use the compiler to adapt lambdas and then call Spark APIs.

**SparkExecutor setup:**

```
CrossPlatformExecutor   SparkExecutor
─────────────────────   ─────────────

execute(SparkStage)
    │
    └──► SparkPlatform.getExecutorFactory()
              └──► new SparkExecutor(platform, job)
                        ├── platform.getSparkContext(job)
                        │     → SparkContext.getOrCreate()   ← connects to cluster
                        └── SparkSession.builder().getOrCreate()
```

```java
// SparkExecutor.java
public SparkExecutor(SparkPlatform platform, Job job) {
    super(job);
    this.sparkContextReference = this.platform.getSparkContext(job);
    this.sc = this.sparkContextReference.get();          // JavaSparkContext
    this.ss = SparkSession.builder()
                .sparkContext(this.sc.sc()).getOrCreate(); // SparkSession
}
```

---

**Task 1: SparkTextFileSource** (no compiler needed — no user lambda)

```
SparkExecutor              SparkTextFileSource         Spark Cluster
─────────────              ───────────────────         ─────────────

execute(task: TextFileSource)
    │
    └──► SparkTextFileSource.evaluate([], [outputRdd], sparkExecutor)
              │
              ├── sparkExecutor.sc.textFile("hdfs://data/input.txt")
              │     → JavaRDD<String> created    ← LAZY, no data read yet
              └── outputRddChannel.accept(rdd)
                    → RddChannel now holds: JavaRDD<String>
```

```java
// SparkTextFileSource.java
RddChannel.Instance output = (RddChannel.Instance) outputs[0];
final JavaRDD<String> rdd = sparkExecutor.sc.textFile(this.getInputUrl());
output.accept(rdd, sparkExecutor);  // store in channel — no execution yet
```

---

**Task 2: SparkFilterOperator** (uses compiler to adapt predicate)

```
SparkExecutor              SparkFilterOperator         FunctionCompiler         PredicateAdapter
─────────────              ───────────────────         ────────────────         ────────────────

execute(task: FilterOperator)
    │
    └──► SparkFilterOperator.evaluate([inputRdd], [outputRdd], sparkExecutor)
              │
              ├── sparkExecutor.getCompiler().compile(predicateDescriptor)
              │         │
              │         └──► predicateDescriptor.getJavaImplementation()  → Java Predicate<T>
              │                   └──► new PredicateAdapter(javaPredicate) → Spark Function<T,Boolean>
              │
              ├── inputRdd = inputRddChannel.provideRdd()       ← reads from channel
              ├── outputRdd = inputRdd.filter(filterFunction)   ← LAZY, just extends DAG
              └── outputRddChannel.accept(outputRdd)            ← writes to channel
```

```java
// SparkFilterOperator.java — evaluate()
final Function<Type, Boolean> filterFunction = sparkExecutor.getCompiler().compile(
        this.predicateDescriptor, this, operatorContext, inputs
);
final JavaRDD<Type> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();
final JavaRDD<Type> outputRdd = inputRdd.filter(filterFunction); // LAZY
((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);
return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
```

```java
// FunctionCompiler.java — compile() for PredicateDescriptor
public <Type> Function<Type, Boolean> compile(PredicateDescriptor<Type> predicateDescriptor, ...) {
    final Predicate<Type> javaImplementation = predicateDescriptor.getJavaImplementation();
    // simple case: wrap in PredicateAdapter
    return new PredicateAdapter<>(javaImplementation);
}
```

```java
// PredicateAdapter.java — the actual wrapper (trivially simple)
public class PredicateAdapter<T> implements Function<T, Boolean> {  // Spark's Function
    private Predicate<T> predicate;  // user's Java lambda

    @Override
    public Boolean call(T dataQuantum) throws Exception {
        return this.predicate.test(dataQuantum);  // just delegates
    }
}
```

---

**Task 3: SparkReduceByOperator** (uses compiler twice — key + reduce)

```
SparkExecutor              SparkReduceByOperator        FunctionCompiler
─────────────              ─────────────────────        ────────────────

execute(task: ReduceByOperator)
    │
    └──► SparkReduceByOperator.evaluate([inputRdd], [outputRdd], sparkExecutor)
              │
              ├── compiler.compileToKeyExtractor(keyDescriptor)
              │     → KeyExtractor (PairFunction<T, Key, T>)     ← wraps key lambda
              ├── compiler.compile(reduceDescriptor)
              │     → BinaryOperatorAdapter (Function2<T,T,T>)   ← wraps reduce lambda
              │
              ├── pairRdd    = inputRdd.mapToPair(keyExtractor)          (LAZY)
              ├── reducedRdd = pairRdd.reduceByKey(reduceFunc, partitions)(LAZY — shuffle later)
              ├── outputRdd  = reducedRdd.map(TupleConverter)            (LAZY)
              └── outputRddChannel.accept(outputRdd)
                    → RddChannel holds full DAG: read → filter → reduceByKey
                    → NOTHING has executed yet
```

```java
// SparkReduceByOperator.java
final JavaRDD<Type> inputStream = input.provideRdd();
final PairFunction<Type, KeyType, Type> keyExtractor =
    sparkExecutor.getCompiler().compileToKeyExtractor(this.keyDescriptor);
Function2<Type, Type, Type> reduceFunc =
    sparkExecutor.getCompiler().compile(this.reduceDescriptor, ...);

final JavaPairRDD<KeyType, Type> pairRdd =
    inputStream.mapToPair(keyExtractor);           // LAZY
final JavaPairRDD<KeyType, Type> reducedPairRdd =
    pairRdd.reduceByKey(reduceFunc, sparkExecutor.getNumDefaultPartitions()); // LAZY
final JavaRDD<Type> outputRdd =
    reducedPairRdd.map(new TupleConverter<>());    // LAZY

output.accept(outputRdd, sparkExecutor);
return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
```

---

### Phase 4 — Action: Actual Spark Execution

All previous `evaluate()` calls built the DAG lazily. The first **action**
triggers real distributed computation across the Spark cluster.

```
SparkExecutor          SparkCollectOperator        Spark Cluster (workers)
─────────────          ────────────────────        ───────────────────────

execute(task: Collect)
    │
    └──► SparkCollectOperator.evaluate([inputRdd], [outputCollection])
              │
              ├── rdd.collect()    ← THIS IS THE SPARK ACTION
              │       │
              │       └──────────────────────────────────────► Spark DAG executes:
              │                                                  Stage 1: textFile()
              │                                                    workers read HDFS partitions
              │                                                  Stage 2: filter()
              │                                                    workers apply predicate
              │                                                  Stage 3: mapToPair() + reduceByKey()
              │                                                    shuffle across workers
              │                                                    each worker reduces its partition
              │                                                  Driver collects results
              │       ◄─────────────────────────────────────── List<T> on driver
              │
              ├── output.accept(collectedList)  → CollectionChannel holds results
              └── containsAction() = true       → SparkExecutor.numActions++

User receives: Iterable<T>  ← backed by the in-memory List
```

```java
// SparkCollectOperator.java
public Tuple<...> evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, ...) {
    RddChannel.Instance input = (RddChannel.Instance) inputs[0];
    CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

    final List<Type> collectedRdd = (List<Type>) input.provideRdd().collect(); // ACTION
    output.accept(collectedRdd);

    return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
}

@Override
public boolean containsAction() {
    return true;  // marks this operator as triggering real Spark execution
}
```

---

## 6. Lazy vs. Eager: The Core Concept

Most Spark operators return `modelLazyExecution()`. Only actions return
`modelEagerExecution()`. This directly maps to Spark's own lazy evaluation model:

```
Operator                containsAction()    When does data actually move?
────────                ────────────────    ─────────────────────────────
SparkTextFileSource     false               Not until an action
SparkFilterOperator     false               Not until an action
SparkReduceByOperator   false               Not until an action
SparkMapOperator        false               Not until an action
SparkCollectOperator    true  ← ACTION      RIGHT NOW — triggers the whole DAG
SparkTextFileSink       true  ← ACTION      RIGHT NOW — writes to disk
```

---

## 7. Key Contrasts: Spark vs. JDBC

```
                    JDBC (Postgres)              Spark
                    ───────────────              ─────
Operators:          3 (filter, project, join)    30+ operators
Operator role:      createSqlClause() → String   evaluate() → builds RDD DAG
Lambda support:     SQL string only              Any Java/Scala/Python lambda
Execution model:    One SQL string to DB         Lazy DAG, triggered by action
Channel type:       SqlQueryChannel (SQL string) RddChannel (RDD reference)
Computation runs:   On the DB server             On Spark workers (distributed)
Fixed startup cost: Low (JDBC connection ~ms)    High (SparkContext startup ~seconds)
Scalability:        Single DB server             Scales to 1000s of workers
Best for:           Structured SQL, small-medium Large-scale, arbitrary computation
Has compiler:       No (SQL string IS the fn)    Yes — wraps Java lambda → Spark fn
```
