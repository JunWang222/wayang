<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Weekly Progress Report: Presto Integration via Generic JDBC

## Summary

This week I started a clean Presto integration branch from `main` and
implemented a local Presto test environment plus an initial Wayang Generic JDBC
connector for Presto.

The current Presto integration test passes end to end:

```text
Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

## What Was Done

Created branch:

```text
presto-jdbc
```

Implemented local Presto setup:

```text
presto-setup/
├── docker-compose.yml
├── presto/catalog/memory.properties
├── scripts/init.sql
└── README.md
```

Added Presto JDBC dependency:

```text
com.facebook.presto:presto-jdbc:0.297
```

Implemented Presto-specific Generic JDBC classes:

```text
Presto.java
PrestoPlatform.java
PrestoPlugin.java
PrestoExecutor.java
PrestoTableSource.java
PrestoFilterOperator.java
PrestoProjectionOperator.java
PrestoFilterMapping.java
PrestoProjectionMapping.java
PrestoChannelConversions.java
wayang-presto-defaults.properties
```

Also updated the Generic JDBC template to support engine-specific config
namespaces and removed trailing semicolons from generated SQL.

## Testing and Verification

Implemented `PrestoGenericJdbcIT`.

The test creates `memory.sales.orders` with 20 rows and verifies:

- plugin registration
- platform binding
- raw Presto JDBC connectivity
- Wayang-generated SQL appears in Presto `system.runtime.queries`
- table scan
- filter pushdown
- projection pushdown
- filter + projection

The most important verification is two-layered:

```text
Presto query history contains Wayang-generated SQL
direct Presto JDBC result == Wayang through Presto result
```

Example query-history proof:

```text
[PRESTO VERIFY] Wayang SQL appears in Presto query history
Expected Wayang-generated SQL:
  SELECT * FROM orders WHERE product = 'server' AND amount > 2000
Presto history count before Wayang execution: 1
Presto history count after Wayang execution : 2
Wayang result rows: 2
```

## Documentation Added

Added:

```text
guides/setup/generic-jdbc-local-engines.md
guides/setup/presto-generic-jdbc-implementation.md
guides/setup/presto-weekly-progress-report.md
presto-setup/README.md
```

## Issues Encountered

Docker image pull initially failed due to unstable Docker Hub access. Retrying
later worked.

One test expectation was wrong at first: APAC row count was expected as `6`, but
the dataset contains `7`. The assertion was corrected.

The initial Presto test only showed pass/fail. I added direct JDBC comparison
and Presto query-history verification to make the result easier to interpret.

## Current Gaps

Current coverage:

- table scan
- filter pushdown
- projection pushdown
- filter + projection

Not covered yet:

- joins
- aggregation pushdown
- limit/order pushdown
- schema/catalog discovery
- benchmark-based cost model tuning
- persistent external storage such as Hive/Iceberg/S3

## Next Steps

1. Review the Presto implementation for consistency with the Trino pattern.
2. Add more tests around SQL generation and failure behavior.
3. Consider adding a Presto demo class/script.
4. Evaluate shared abstractions between Presto and Trino.
5. Explore aggregation or join support.
6. Prepare the branch for review and cleanup before broader integration.
