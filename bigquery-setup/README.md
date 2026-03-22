# BigQuery Local Setup

Local BigQuery emulator for development and integration testing.

## Stack

| Component | Image | Port | Role |
|-----------|-------|------|------|
| **BigQuery Emulator** | `ghcr.io/goccy/bigquery-emulator:0.6.6` | 9050 (HTTP) / 9060 (gRPC) | BigQuery-compatible SQL engine |

Single container. Data is seeded from `data.yaml` on startup and lives in memory.

## Directory Layout

```
bigquery-setup/
├── docker-compose.yml          # Emulator container
├── data.yaml                   # Seed data (test-project.sales.orders)
├── pom.xml                     # Standalone Maven project
└── src/test/java/.../
    └── BigQueryEmulatorIT.java # JUnit 5 integration tests
```

## Quick Start

### 1. Start the emulator

```bash
cd bigquery-setup
docker-compose up -d
```

The emulator starts in ~2 seconds. Data from `data.yaml` is loaded automatically.

### 2. Run integration tests

```bash
mvn test -Pintegration
```

### 3. Manual exploration

Query via curl:

```bash
curl -s -X POST \
  "http://localhost:9050/bigquery/v2/projects/test-project/queries" \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM sales.orders LIMIT 5", "useLegacySql": false}' \
  | python3 -m json.tool
```

### 4. Tear down

```bash
docker-compose down
```

## Test Coverage

| Test | What it checks |
|------|----------------|
| `testDatasetVisible` | `sales` dataset exists |
| `testFullScan` | Full table scan, 10 rows |
| `testFilterByRegion` | `WHERE region = 'APAC'` |
| `testFilterByAmount` | `WHERE amount > 1000` |
| `testAggregation` | `GROUP BY region` + `SUM(amount)` |
| `testProjection` | `SELECT region, product LIMIT 5` |
| `testCount` | `SELECT count(*)` — used by Wayang for cardinality estimation |

## Environment Variables

```bash
BIGQUERY_HOST=http://localhost:9050 mvn test -Pintegration
```

## Notes

- Tests use `google-cloud-bigquery` client library (REST-based, no JDBC).
- The client connects with `NoCredentials` — no GCP account needed.
- The BigQuery JDBC driver (`google-cloud-bigquery-jdbc`) requires OAuth even against the emulator, so JDBC-based tests are not included yet.
