# Trino + BigQuery — Local Setup, Run, and Test

End-to-end guide for bringing up the Wayang Trino and BigQuery connectors on a
fresh machine. Branch: [`trino-jdbc`](https://github.com/JunWang222/wayang/tree/trino-jdbc).

Both connectors live in the same module
([`wayang-platforms/wayang-generic-jdbc`](../../wayang-platforms/wayang-generic-jdbc/))
under packages `org.apache.wayang.genericjdbc.trino` and
`org.apache.wayang.genericjdbc.bigquery`. They share the `genericjdbc` runtime
(executor, channels, mappings) and only the platform-specific operators and
default properties differ.

---

## 0. Quickstart (TL;DR)

```bash
git clone https://github.com/JunWang222/wayang.git
cd wayang
git checkout trino-jdbc

# 1. Build the module once
mvn -DskipTests -Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true install

# 2. Trino — full demo (docker up → seed → CLI queries → Wayang API)
./demo-trino.sh

# 3. BigQuery — sample-data demo (no creds needed)
./demo-bigquery.sh
#    or, with credentials:
export BQ_PROJECT=daeproject-316010
export BQ_URL='jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=daeproject-316010;OAuthType=0;OAuthServiceAcctEmail=wayang-bq-test@daeproject-316010.iam.gserviceaccount.com;OAuthPvtKeyPath='"$HOME/wayang-bq-key.json"
./demo-bigquery.sh --live
```

The rest of this guide explains each piece in detail.

---

## 1. Prerequisites

### 1.1 Host tools

| Tool | Version | Install hint |
|------|---------|--------------|
| **JDK 17** | 17.x | macOS: `brew install openjdk@17` · Linux: `apt install openjdk-17-jdk` |
| **Maven** | 3.8+ | `brew install maven` / `apt install maven` · verify with `mvn -v` |
| **Docker** | recent | Docker Desktop (Mac/Win) or Docker Engine + Compose plugin (Linux). Allocate **≥ 6 GB RAM, ≥ 4 CPUs** — the Trino stack needs it. |
| **Git** | any | for cloning the repo |
| **`gcloud` + `bq` CLI** | latest | only for live BigQuery — `brew install --cask google-cloud-sdk` |

Verify:

```bash
java -version    # openjdk 17.x
mvn -v           # Apache Maven 3.8+, Java 17
docker info      # daemon running
docker compose version
```

### 1.2 Repo bootstrap

```bash
git clone https://github.com/JunWang222/wayang.git
cd wayang
git checkout trino-jdbc       # contains BOTH Trino and BigQuery connectors
```

The repo has two relevant feature branches on the fork:

| Branch | Contents |
|--------|----------|
| `trino-jdbc` (**use this**) | Trino + BigQuery connectors, demos, this doc |
| `bigquery-jdbc` | BigQuery only, no Trino — strict subset of `trino-jdbc` |

Build once so the local Maven cache has the module:

```bash
mvn -DskipTests -Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true install
```

Maven flags explained: `-Pskip-prerequisite-check` (skip pre-build sanity
checks), `-Drat.skip=true` (skip Apache RAT license-header check —
generated/non-ASF files trip it), `-Dmaven.javadoc.skip=true` (Javadoc is slow
and not needed locally).

---

## 2. Trino

### 2.1 Stack overview

The Docker Compose file at [`trino-setup/docker-compose.yml`](../../trino-setup/docker-compose.yml)
brings up **four containers** that together implement Trino + Iceberg + S3.

| Service | Image | Container name | Host port | Role |
|---------|-------|----------------|-----------|------|
| `trino` | `trinodb/trino:435` | `trino` | **8080** | SQL engine + web UI |
| `metastore` | `naushadh/hive-metastore:latest` | `trino-metastore` | 9083 | Iceberg table catalog (Thrift) |
| `postgres` | `postgres:15-alpine` | `trino-postgres` | 5432 | HMS metadata backing store |
| `minio` | `minio/minio:latest` | `trino-minio` | **9000** (S3), **9001** (UI) | S3-compatible object store for Parquet |
| `minio-init` | `minio/mc:latest` | `trino-minio-init` | – | one-shot: creates the `warehouse` bucket, then exits |

Named volumes: `postgres-data` (HMS DB), `minio-data` (Parquet files).
All containers share Docker Compose's default network, so `metastore` can
reach `postgres:5432` and `minio:9000` by service name.

**Startup order (enforced by `depends_on` + healthchecks):**

```
postgres ──┐                 ┌── trino   (waits for metastore healthy)
           ├── metastore ────┤
minio ─────┤                 └── minio   (already healthy)
           └── minio-init (one-shot, exits 0 → metastore can start)
```

**Healthchecks** (what "healthy" means for each):

- `postgres`: `pg_isready -U hive -d metastore`
- `minio`: HTTP GET `http://localhost:9000/minio/health/live`
- `metastore`: TCP probe `localhost:9083` (image has no `nc`/`curl`, uses bash `/dev/tcp`)
- `trino`: HTTP GET `http://localhost:8080/v1/info`

**Key env vars on `metastore`** (Iceberg/MinIO wiring):

```yaml
DATABASE_HOST: postgres
DATABASE_DB:   metastore
DATABASE_USER: hive
DATABASE_PASSWORD: hive
S3_ENDPOINT_URL: http://minio:9000
S3_BUCKET:       warehouse
AWS_ACCESS_KEY_ID:     minioadmin
AWS_SECRET_ACCESS_KEY: minioadmin
REGION: us-east-1
```

**Trino catalog configs** (mounted into the container at
`/etc/trino/catalog/`):

- [`trino/catalog/iceberg.properties`](../../trino-setup/trino/catalog/iceberg.properties) — Iceberg connector pointing at HMS + MinIO
- [`trino/catalog/tpch.properties`](../../trino-setup/trino/catalog/tpch.properties) — built-in TPC-H (no storage)
- [`trino/config.properties`](../../trino-setup/trino/config.properties) — single-node coordinator config

### 2.2 Directory layout

```
trino-setup/
├── docker-compose.yml          # 4-service stack above
├── trino/
│   ├── config.properties       # Trino node config (single-node coordinator)
│   └── catalog/
│       ├── iceberg.properties  # Iceberg via HMS + MinIO
│       └── tpch.properties     # Built-in TPC-H (no storage needed)
├── hive/                       # (HMS image config — usually untouched)
├── scripts/
│   ├── init.sql                # CREATE iceberg.sales.orders + 20 sample rows
│   └── run-init.sh             # Helper: waits for Trino, then runs init.sql
├── demo.sh                     # Self-contained demo (must be run from trino-setup/)
├── pom.xml                     # Standalone Maven project for the IT (Java 17)
└── src/test/java/.../TrinoIntegrationTest.java
```

### 2.3 Start the stack

```bash
cd trino-setup
docker compose up -d
docker compose ps               # wait until all show "(healthy)"
```

Typical first-run cold start: 30–60 s (images pull on first invocation).

Open the UIs to sanity-check:
- Trino: <http://localhost:8080>
- MinIO: <http://localhost:9001> (login `minioadmin` / `minioadmin`)

### 2.4 Seed sample Iceberg data

```bash
./scripts/run-init.sh           # from trino-setup/
```

This runs [`scripts/init.sql`](../../trino-setup/scripts/init.sql) which:
1. `CREATE SCHEMA IF NOT EXISTS iceberg.sales`
2. `CREATE TABLE iceberg.sales.orders (order_id, region, product, amount, order_date)` in Parquet
3. `DELETE FROM ...` (idempotent — safe to re-run)
4. `INSERT INTO ...` 20 sample rows (4 regions: AMER/APAC/EMEA/LATAM, 5 products)

Verify:

```bash
docker exec -it trino trino --execute \
  "SELECT region, COUNT(*) AS n, SUM(amount) AS total
   FROM iceberg.sales.orders GROUP BY region ORDER BY total DESC"
```

You should also see Parquet files in MinIO under `warehouse/sales/orders/`.

### 2.5 Run the connector demo

From repo root:

```bash
./demo-trino.sh
```

Three-act walk-through:
1. **Act 1** — `docker compose up -d`, run `init.sql`, list running containers.
2. **Act 2** — direct Trino-CLI queries (full scan, filter `WHERE region='AMER'`, projection `SELECT region, product, amount`).
3. **Act 3** — same operators via Wayang API. Runs `TrinoDemo.main()`, which
   builds a `WayangPlan` of `TrinoTableSource → FilterOperator → sink`, lets
   the optimizer rewrite logical → `TrinoFilterOperator` /
   `TrinoProjectionOperator`, and pushes a SQL string down via JDBC.

Run `TrinoDemo` manually (without the bash scaffolding):

```bash
mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
  -Dexec.mainClass=org.apache.wayang.genericjdbc.TrinoDemo \
  -Dtrino.url=jdbc:trino://localhost:8080 \
  -Dtrino.user=admin \
  -Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true
```

### 2.6 Connector defaults and config keys

Defaults live in
[`wayang-trino-defaults.properties`](../../wayang-platforms/wayang-generic-jdbc/src/main/resources/wayang-trino-defaults.properties).
Override via system properties (`-Dwayang.trino.jdbc.url=...`) or a
`wayang.properties` on the classpath.

| Key | Default | Purpose |
|-----|---------|---------|
| `wayang.trino.jdbc.url` | (unset) | `jdbc:trino://host:port` |
| `wayang.trino.jdbc.user` | (unset) | `admin` for local |
| `wayang.trino.jdbc.password` | (unset) | empty for local |
| `wayang.trino.jdbc.driverName` | `io.trino.jdbc.TrinoDriver` | loaded via reflection |
| `wayang.trino.cpu.mhz` / `.cores` | `2700` / `4` | hardware profile for cost model |
| `wayang.trino.tablesource.load` | `cpu = 10*out + 800k` | distributed scan: low α, mid β |
| `wayang.trino.filter.load` | `cpu = 10*in + 800k` | pushed-down WHERE |
| `wayang.trino.projection.load` | `cpu = 10*in + 800k` | pushed-down SELECT cols |
| `wayang.trino.sqltostream.load.query` | `cpu = 10*out + 800k` | SQL → JDBC streaming |

### 2.7 Integration test (`TrinoGenericJdbcIT`)

```bash
mvn test -pl wayang-platforms/wayang-generic-jdbc \
  -Dtest=TrinoGenericJdbcIT \
  -Pintegration,skip-prerequisite-check \
  -Drat.skip=true -Dmaven.javadoc.skip=true
```

Requires the Docker stack to be up and seeded. The tests assert that queries
land in Trino's `system.runtime.queries` history — that's the proof that
pushdown actually reaches Trino (rather than Wayang silently falling back to
the Java executor).

### 2.8 Tear down

```bash
cd trino-setup
docker compose down              # stop containers, keep volumes
docker compose down -v           # also delete postgres-data, minio-data (clears Iceberg files)
```

---

## 3. BigQuery

Two ways to use the BigQuery connector locally:

| Mode | When to use | What you need |
|------|-------------|---------------|
| **Emulator** ([`bigquery-setup/`](../../bigquery-setup/)) | iterating on connector code without GCP | Docker only |
| **Live BigQuery** (`demo-bigquery.sh --live`, `BigQueryGenericJdbcIT`) | exercising the real JDBC driver, OAuth, pricing path | GCP project, service-account key, `bq` CLI |

### 3.1 Emulator path

#### Stack

| Service | Image | Container name | Host ports | Role |
|---------|-------|----------------|------------|------|
| `bigquery` | `ghcr.io/goccy/bigquery-emulator:0.6.6` (`linux/amd64`) | `bigquery-emulator` | **9050** (HTTP REST), **9060** (gRPC Storage) | BigQuery-compatible engine |

Single container, no metastore, no object store. Data is loaded from
[`data.yaml`](../../bigquery-setup/data.yaml) on startup (10-row
`test-project.sales.orders` dataset) and lives in memory — restart = empty.

The compose command passes:

```yaml
command: --project=test-project --data-from-yaml=/data.yaml
healthcheck:
  test: wget -q --spider http://localhost:9050/bigquery/v2/projects/test-project/datasets
```

Volume mount: `./data.yaml:/data.yaml` (read-only seed).

> **Why the platform pin?** The emulator image is built for `linux/amd64`
> only — on Apple Silicon you'll get a "no matching manifest" or
> emulation-via-Rosetta. The `platform: linux/amd64` in the compose file
> handles that; expect slower throughput on M-series Macs.

#### Run

```bash
cd bigquery-setup
docker compose up -d                # ~2s startup
mvn test -Pintegration              # REST-based, no creds
docker compose down
```

Manual query (REST):

```bash
curl -s -X POST \
  "http://localhost:9050/bigquery/v2/projects/test-project/queries" \
  -H "Content-Type: application/json" \
  -d '{"query":"SELECT * FROM sales.orders LIMIT 5","useLegacySql":false}' \
  | python3 -m json.tool
```

> **Note on JDBC vs emulator:** Google's official BigQuery JDBC driver
> requires OAuth even when pointed at the emulator, so the emulator's
> integration test
> ([`BigQueryEmulatorIT.java`](../../bigquery-setup/src/test/java/org/apache/wayang/bigquery/BigQueryEmulatorIT.java))
> uses the REST `google-cloud-bigquery` client library instead. The
> **generic-jdbc** BigQuery integration test (`BigQueryGenericJdbcIT`) goes
> through the JDBC driver and therefore only works against live BigQuery.

### 3.2 Live BigQuery path

Used by `demo-bigquery.sh --live` and `BigQueryGenericJdbcIT`. Sample dataset
is `daeproject-316010.sales.orders` (20 rows, mirrors the Trino Iceberg seed).

#### 3.2.1 One-time GCP setup

1. **Install gcloud + bq:**
   ```bash
   brew install --cask google-cloud-sdk     # macOS
   gcloud --version && bq version
   ```
2. **Authenticate as your user** (interactive, one-time):
   ```bash
   gcloud auth login
   gcloud config set project daeproject-316010
   ```
3. **Service account** — `BigQueryGenericJdbcIT` hard-codes:
   - project: `daeproject-316010`
   - service account: `wayang-bq-test@daeproject-316010.iam.gserviceaccount.com`
   - key path: `~/wayang-bq-key.json`

   To create the service account fresh:
   ```bash
   gcloud iam service-accounts create wayang-bq-test \
     --display-name "Wayang BigQuery test SA" \
     --project daeproject-316010
   gcloud projects add-iam-policy-binding daeproject-316010 \
     --member="serviceAccount:wayang-bq-test@daeproject-316010.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataViewer"
   gcloud projects add-iam-policy-binding daeproject-316010 \
     --member="serviceAccount:wayang-bq-test@daeproject-316010.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"
   gcloud iam service-accounts keys create ~/wayang-bq-key.json \
     --iam-account=wayang-bq-test@daeproject-316010.iam.gserviceaccount.com
   chmod 600 ~/wayang-bq-key.json
   ```

   **If the service account already exists on your current laptop**, the
   simpler path is to just copy the key:
   ```bash
   scp ~/wayang-bq-key.json other-laptop:~/wayang-bq-key.json
   ssh other-laptop chmod 600 ~/wayang-bq-key.json
   ```

   > Never commit the key. Already gitignored implicitly — keep it that way.

4. **Verify dataset access:**
   ```bash
   bq query --use_legacy_sql=false \
     'SELECT COUNT(*) FROM `daeproject-316010.sales.orders`'
   ```
   Expect `20`. If the table doesn't exist (e.g. you're using a different
   GCP project), recreate it with the schema from
   [`trino-setup/scripts/init.sql`](../../trino-setup/scripts/init.sql) —
   types map cleanly: `BIGINT→INT64`, `VARCHAR→STRING`, `DOUBLE→FLOAT64`,
   `DATE→DATE`.

#### 3.2.2 Install the BigQuery JDBC driver

The driver (`com.google.cloud.bigquery.jdbc.BigQueryDriver`) is **not on Maven
Central**. Download Google's distribution from
<https://cloud.google.com/bigquery/docs/reference/odbc-jdbc-drivers> and
install to the local Maven repo:

```bash
# Adjust filename/version to whatever you downloaded
mvn install:install-file \
  -Dfile=SimbaJDBCDriverforGoogleBigQuery42_<version>.jar \
  -DgroupId=com.google.cloud.bigquery \
  -DartifactId=google-cloud-bigquery-jdbc \
  -Dversion=<version> \
  -Dpackaging=jar
```

Pin the **same `<version>`** as the
[`pom.xml`](../../wayang-platforms/wayang-generic-jdbc/pom.xml) on this
branch already references. Check resolved version with:

```bash
mvn dependency:tree -pl wayang-platforms/wayang-generic-jdbc | grep bigquery-jdbc
```

#### 3.2.3 Run the demo

```bash
export BQ_PROJECT=daeproject-316010
export BQ_URL='jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=daeproject-316010;OAuthType=0;OAuthServiceAcctEmail=wayang-bq-test@daeproject-316010.iam.gserviceaccount.com;OAuthPvtKeyPath='"$HOME/wayang-bq-key.json"

./demo-bigquery.sh --live
```

Without `--live`, the script falls back to printed sample data + cost-model
demo (still useful for showing optimizer behaviour without GCP).

Manual invocation of `BigQueryDemo`:

```bash
mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
  -Dexec.mainClass=org.apache.wayang.genericjdbc.BigQueryDemo \
  -Dbigquery.mode=cost       \
  -Dbigquery.project="$BQ_PROJECT" \
  -Pskip-prerequisite-check -Drat.skip=true -Dmaven.javadoc.skip=true
# bigquery.mode = cost | filter | projection
# add -Dbigquery.url="$BQ_URL" for filter/projection modes
```

### 3.3 Connector defaults and config keys

Defaults in
[`wayang-bigquery-defaults.properties`](../../wayang-platforms/wayang-generic-jdbc/src/main/resources/wayang-bigquery-defaults.properties).
Same shape as Trino, but cost model parameters reflect BigQuery's much higher
fixed dispatch cost:

| Key | Default | Notes |
|-----|---------|-------|
| `wayang.bigquery.jdbc.driverName` | `com.google.cloud.bigquery.jdbc.BigQueryDriver` | |
| `wayang.bigquery.jdbc.url` | (unset) | required per-deployment |
| `wayang.bigquery.cpu.mhz` / `.cores` | `2700` / `8` | serverless — model max parallelism |
| `wayang.bigquery.tablesource.load` | `cpu = 5*out + 2_000_000` | β=2M reflects dispatch latency |
| `wayang.bigquery.filter.load` | `cpu = 5*in  + 2_000_000` | |
| `wayang.bigquery.projection.load` | `cpu = 5*in  + 2_000_000` | |

Crossover points (from the inline notes):
- BigQuery vs Postgres: BigQuery wins for `n > ~32k rows`
- BigQuery vs Trino: BigQuery wins for `n > ~240k rows`

### 3.4 Integration test (`BigQueryGenericJdbcIT`)

```bash
mvn test -pl wayang-platforms/wayang-generic-jdbc \
  -Dtest=BigQueryGenericJdbcIT \
  -Pintegration,skip-prerequisite-check \
  -Drat.skip=true -Dmaven.javadoc.skip=true
```

The test self-skips if it can't open a JDBC connection (missing key,
unreachable network, missing driver), so missing config produces a no-op
rather than a hard failure.

---

## 4. Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `Connection refused` on `localhost:8080` | Trino not ready yet | `docker compose ps` — wait for `(healthy)`; cold-start is 30–60s. |
| `docker compose ps` shows `metastore` not healthy | MinIO bucket race | `docker compose down -v && docker compose up -d`. |
| Iceberg query returns 0 rows | Stack came up but `init.sql` wasn't run | `./trino-setup/scripts/run-init.sh` |
| MinIO UI shows no Parquet files | Same as above | run `init.sql`; check `warehouse/sales/orders/` |
| `Address already in use` on 8080/9000/9001/5432/9083 | Conflict with host service | Stop the conflicting service, or edit `docker-compose.yml` port mappings. |
| Docker pull fails for `naushadh/hive-metastore` on Apple Silicon | Image is `linux/amd64` only | Already runs under Rosetta; ensure Docker Desktop has "Use Rosetta for x86/amd64 emulation on Apple Silicon" enabled. |
| BigQuery emulator: "no matching manifest for linux/arm64" | Same — emulator is `amd64` only | `platform: linux/amd64` is already set in the compose file; enable Rosetta in Docker Desktop. |
| `ClassNotFoundException: com.google.cloud.bigquery.jdbc.BigQueryDriver` | JDBC JAR not installed to local Maven repo | See §3.2.2; install with `mvn install:install-file`. |
| BigQuery: `403 Permission denied` | Service account lacks roles | Grant `roles/bigquery.dataViewer` **and** `roles/bigquery.jobUser` to `wayang-bq-test@…`. |
| `BigQueryGenericJdbcIT` reports "skipped" | Key not at `~/wayang-bq-key.json`, or URL unreachable | Check `ls -l ~/wayang-bq-key.json` and that the SA still has access. |
| `TrinoGenericJdbcIT`: "no SQL in query history" | Wayang fell back to Java executor | Confirm `wayang.trino.jdbc.url` is set; the test prints the URL it used. |
| `mvn exec:java` says "module not found" | Forgot to `mvn install` once after clone | Re-run `mvn -DskipTests … install` from §1.2. |
| Apple Silicon: slow Trino | Running x86 images under Rosetta | Acceptable for dev. For speed, develop on amd64 or use a remote Linux VM. |

---

## 5. Reference — what lives where

| Path | Purpose |
|------|---------|
| [`wayang-platforms/wayang-generic-jdbc/`](../../wayang-platforms/wayang-generic-jdbc/) | Connector module (Trino + BigQuery, shared genericjdbc runtime) |
| [`.../genericjdbc/trino/`](../../wayang-platforms/wayang-generic-jdbc/src/main/java/org/apache/wayang/genericjdbc/trino/) | Trino platform, operators, mappings |
| [`.../genericjdbc/bigquery/`](../../wayang-platforms/wayang-generic-jdbc/src/main/java/org/apache/wayang/genericjdbc/bigquery/) | BigQuery platform, operators, mappings |
| [`.../genericjdbc/TrinoDemo.java`](../../wayang-platforms/wayang-generic-jdbc/src/main/java/org/apache/wayang/genericjdbc/TrinoDemo.java) | Standalone Java main exercising filter + projection pushdown |
| [`.../genericjdbc/BigQueryDemo.java`](../../wayang-platforms/wayang-generic-jdbc/src/main/java/org/apache/wayang/genericjdbc/BigQueryDemo.java) | Same for BigQuery, with `cost`/`filter`/`projection` modes |
| [`.../wayang-trino-defaults.properties`](../../wayang-platforms/wayang-generic-jdbc/src/main/resources/wayang-trino-defaults.properties) | Trino cost model + JDBC defaults |
| [`.../wayang-bigquery-defaults.properties`](../../wayang-platforms/wayang-generic-jdbc/src/main/resources/wayang-bigquery-defaults.properties) | BigQuery cost model + JDBC defaults |
| [`.../TrinoGenericJdbcIT.java`](../../wayang-platforms/wayang-generic-jdbc/src/test/java/org/apache/wayang/genericjdbc/TrinoGenericJdbcIT.java) | JUnit IT — pushdown via Wayang → assertion against Trino query history |
| [`.../BigQueryGenericJdbcIT.java`](../../wayang-platforms/wayang-generic-jdbc/src/test/java/org/apache/wayang/genericjdbc/BigQueryGenericJdbcIT.java) | JUnit IT — pushdown via Wayang → live BigQuery |
| [`trino-setup/`](../../trino-setup/) | Docker stack (Trino + HMS + Postgres + MinIO) + seed scripts |
| [`bigquery-setup/`](../../bigquery-setup/) | Docker BigQuery emulator + seed data |
| [`demo-trino.sh`](../../demo-trino.sh) | Top-level 3-act Trino demo (run from repo root) |
| [`demo-bigquery.sh`](../../demo-bigquery.sh) | Top-level 3-act BigQuery demo (sample-data or `--live`) |
