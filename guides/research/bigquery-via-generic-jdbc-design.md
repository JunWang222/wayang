# BigQuery via GenericJdbc — Design Document

> Connect BigQuery to Wayang by leveraging `wayang-generic-jdbc`, mirroring the Trino approach.

---

## 1. Background

BigQuery is Google's fully managed, serverless data warehouse for OLAP workloads. It supports standard SQL, columnar storage, and auto-scaling — making it a natural fit as a Wayang backend alongside Trino and Spark.

Like Trino, BigQuery exposes a JDBC interface. Google recently released an open-source JDBC driver on Maven Central (`com.google.cloud:google-cloud-bigquery-jdbc`), replacing the older proprietary Simba driver.

For local development and testing, the community-maintained [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) provides a Docker-based BigQuery server backed by ZetaSQL (Google's own SQL parser).

---

## 2. Architecture: two integration paths

### Path A — Google BigQuery Java client library (standalone tests)

```
BigQuery Emulator (:9050 HTTP, :9060 gRPC)
     ▲
     │  REST API / gRPC
     │
google-cloud-bigquery client library
     │  set endpoint → http://localhost:9050
     │  set credentials → NoCredentials
     │
Integration test (SQL via client.query())
```

- The `google-cloud-bigquery` client (`com.google.cloud:google-cloud-bigquery`) is on Maven Central.
- It supports pointing at a custom endpoint + `NoCredentials` for emulator use.
- No JDBC needed — tests run directly against the BigQuery REST API.
- **Use this for standalone integration tests** (validating the emulator, SQL dialect, data loading).

### Path B — GenericJdbc with BigQuery JDBC driver (Wayang integration)

```
BigQuery (cloud or emulator)
     ▲
     │  JDBC
     │
google-cloud-bigquery-jdbc (driver class: com.google.cloud.bigquery.jdbc.BQDriver)
     │
GenericJdbcPlatform → GenericJdbcExecutor → SQL assembly
     │
Wayang pipeline
```

- The Google JDBC driver (`com.google.cloud:google-cloud-bigquery-jdbc:0.6.0`) is on Maven Central.
- Connection URL: `jdbc:bigquery://https://HOST:PORT;ProjectId=PROJECT_ID;OAuthType=AUTH_TYPE;...`
- **Complication**: The JDBC driver requires OAuth authentication — even against the emulator. There is no `NoCredentials` bypass for the JDBC driver (unlike the REST client). This means JDBC-based tests against the emulator require a mock credential setup.
- **Use this for Wayang GenericJdbc integration** (once auth is resolved).

### Recommendation

Start with **Path A** for integration tests (no auth hassle), then layer **Path B** for Wayang integration against real BigQuery or once the emulator JDBC auth story improves.

---

## 3. Supported operators (via GenericJdbc)

Same as Trino — the GenericJdbc module provides:

| Wayang logical op | GenericJdbc physical op | SQL clause | Notes |
|---|---|---|---|
| `TableSource` | `GenericJdbcTableSource` | `FROM {table}` | BigQuery uses `project.dataset.table` |
| `FilterOperator` | `GenericJdbcFilterOperator` | `WHERE {sql}` | Requires `.withSqlImplementation(...)` |
| `MapOperator` (projection) | `GenericJdbcProjectionOperator` | `SELECT {cols}` | Projection only |

### BigQuery SQL dialect differences

| Feature | Trino | BigQuery |
|---|---|---|
| Table names | `catalog.schema.table` | `` `project.dataset.table` `` (backtick-quoted) |
| String quoting | `'single quotes'` | `'single quotes'` (same) |
| Identifier quoting | `"double quotes"` | `` `backticks` `` |
| Boolean | `true` / `false` | `TRUE` / `FALSE` |
| LIMIT | `LIMIT n` | `LIMIT n` (same) |
| Array access | `arr[1]` | `arr[OFFSET(0)]` |

For simple Filter + Projection pipelines (the GenericJdbc scope), the SQL dialect differences are minimal — standard `WHERE` and `SELECT` clauses work the same way.

---

## 4. Local emulator setup

### Stack

| Component | Image | Port | Role |
|---|---|---|---|
| **BigQuery Emulator** | `ghcr.io/goccy/bigquery-emulator:0.6.6` | 9050 (HTTP), 9060 (gRPC) | BigQuery-compatible SQL engine |

Single container — no metastore, no object storage. Data lives in memory (or optional SQLite file).

### Seed data

The emulator loads initial data from a YAML file:

```yaml
projects:
- id: test-project
  datasets:
  - id: sales
    tables:
    - id: orders
      columns:
      - name: order_id
        type: INTEGER
      - name: region
        type: STRING
      - name: product
        type: STRING
      - name: amount
        type: FLOAT
      data:
      - order_id: 1
        region: APAC
        product: Widget A
        amount: 1500.0
      # ... more rows
```

### Docker compose

```yaml
services:
  bigquery:
    image: ghcr.io/goccy/bigquery-emulator:0.6.6
    container_name: bigquery-emulator
    ports:
      - "9050:9050"   # HTTP (REST API)
      - "9060:9060"   # gRPC (Storage API)
    volumes:
      - ./data.yaml:/data.yaml
    command: --project=test-project --data-from-yaml=/data.yaml
```

---

## 5. GenericJdbc configuration (Path B)

```properties
wayang.bigquery.jdbc.url        = jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=my-project;OAuthType=3
wayang.bigquery.jdbc.user       =
wayang.bigquery.jdbc.password   =
wayang.bigquery.jdbc.driverName = com.google.cloud.bigquery.jdbc.BQDriver
```

For the emulator (once auth is resolved):
```properties
wayang.bigquery.jdbc.url        = jdbc:bigquery://http://localhost:9050;ProjectId=test-project;OAuthType=0;...
```

### User code

```java
TableSource source = new GenericJdbcTableSource(
    "bigquery",                     // jdbcName
    "`test-project.sales.orders`",  // BigQuery requires backtick quoting
    "order_id", "region", "amount"
);

FilterOperator<Record> filter = new FilterOperator<>(
    new PredicateDescriptor<>(
        r -> ((Number) r.getField(2)).doubleValue() > 1000.0, Record.class
    ).withSqlImplementation("amount > 1000")
);
```

Generated SQL:
```sql
SELECT region, amount FROM `test-project.sales.orders` WHERE amount > 1000
```

---

## 6. Differences from the Trino setup

| Aspect | Trino | BigQuery |
|---|---|---|
| Docker complexity | 4 containers (Trino + HMS + MinIO + Postgres) | 1 container (emulator) |
| JDBC driver | `io.trino:trino-jdbc` — no auth needed locally | `google-cloud-bigquery-jdbc` — requires OAuth |
| Test strategy | JDBC integration tests work locally | REST client tests (Path A) work locally; JDBC tests (Path B) need auth workaround |
| Table naming | `catalog.schema.table` | `` `project.dataset.table` `` (backticks) |
| Cardinality estimation | `SELECT count(*)` works | `SELECT count(*)` works on emulator |

---

## 7. Implementation plan

### Phase 1 — Emulator + standalone tests (Path A)

1. `bigquery-setup/docker-compose.yml` with `bigquery-emulator`
2. `data.yaml` with sample `sales.orders` table (same data as Trino)
3. Standalone tests using `google-cloud-bigquery` client library:
   - Basic connectivity
   - Full table scan
   - Filter query
   - Aggregation query
   - Projection query

### Phase 2 — Wayang GenericJdbc integration (Path B)

1. Add `google-cloud-bigquery-jdbc` dependency to `wayang-generic-jdbc/pom.xml`
2. Set `wayang.bigquery.jdbc.*` properties
3. Integration test: `BigQueryGenericJdbcIT.java`
4. Note: requires either real BigQuery credentials or emulator JDBC auth workaround

### Phase 3 — BigQuery-specific considerations

1. Backtick quoting for table names (may need a minor change in `GenericJdbcTableSource`)
2. BigQuery-specific cost model values in `defaults.properties`

---

## 8. Open questions

1. **JDBC + emulator auth**: Can the Google JDBC driver connect to the emulator without real GCP credentials? The REST client can (`NoCredentials`), but the JDBC driver's `OAuthType` may not support this.
2. **Table name quoting**: BigQuery requires backtick-quoted identifiers. If the user passes `` `project.dataset.table` `` as the table name, `GenericJdbcTableSource` will use it as-is in the SQL — this should work but needs testing.
3. **Semicolons**: Already fixed in the Trino branch. Need to cherry-pick or re-apply the fix on this branch.
