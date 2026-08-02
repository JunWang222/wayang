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

# SQL Parquet Join Profiling

This guide prepares a small shared Parquet data set for the
`SqlParquetJoinProfilingIT` integration test. The test registers Trino, Presto,
and BigQuery at the same time, then runs a join over logical `ParquetSource`
inputs instead of platform-local `TableSource` inputs.

## Generate Data

On Windows:

```powershell
.\tools\parquet-profile\generate-shared-parquet-data.ps1
```

This creates:

```text
target/shared-parquet-profile/orders.parquet
target/shared-parquet-profile/customers.parquet
```

The data set is intentionally tiny:

```text
orders_rows=12
customers_rows=5
expected_join_rows=12
```

To upload the same files to GCS:

```powershell
.\tools\parquet-profile\generate-shared-parquet-data.ps1 `
  -GcsPrefix "gs://YOUR_BUCKET/wayang-profile"
```

Use the printed `shared_orders_uri` and `shared_customers_uri` as the canonical
Parquet URIs in the profiling test.

## Schemas

`orders.parquet`:

```text
order_id INT
customer_id INT
region STRING
amount DOUBLE
bucket INT
```

`customers.parquet`:

```text
cust_id INT
tier STRING
```

The profiling join condition is:

```sql
orders.customer_id = customers.cust_id
```

## External Relations

Create one external relation per input in each platform that should execute the
profiling query. Keep the Parquet URI stable across platforms and map it to each
platform relation through `wayang.<platform>.parquetsource.mappings`.

Trino and Presto connector syntax differs by catalog. For Hive/Iceberg-style
catalogs, use this shape and adjust the catalog/schema names:

```sql
CREATE TABLE IF NOT EXISTS iceberg.profile.orders_ext (
  order_id INTEGER,
  customer_id INTEGER,
  region VARCHAR,
  amount DOUBLE,
  bucket INTEGER
)
WITH (
  format = 'PARQUET',
  external_location = 'gs://YOUR_BUCKET/wayang-profile/orders.parquet'
);

CREATE TABLE IF NOT EXISTS iceberg.profile.customers_ext (
  cust_id INTEGER,
  tier VARCHAR
)
WITH (
  format = 'PARQUET',
  external_location = 'gs://YOUR_BUCKET/wayang-profile/customers.parquet'
);
```

For BigQuery:

```sql
CREATE EXTERNAL TABLE `YOUR_PROJECT.profile.orders_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/wayang-profile/orders.parquet']
);

CREATE EXTERNAL TABLE `YOUR_PROJECT.profile.customers_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://YOUR_BUCKET/wayang-profile/customers.parquet']
);
```

## Run The Profiling IT

Example for Trino as the execution target:

```powershell
.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform trino `
  -OrdersUri "gs://YOUR_BUCKET/wayang-profile/orders.parquet" `
  -CustomersUri "gs://YOUR_BUCKET/wayang-profile/customers.parquet" `
  -OrdersRelation "iceberg.profile.orders_ext" `
  -CustomersRelation "iceberg.profile.customers_ext" `
  -SinkRelation "iceberg.profile.join_profile_out"
```

Use `-DryRun` to print the environment that the script will set without running
Maven.

The equivalent manual setup is:

```powershell
$env:WAYANG_PROFILE_PARQUET_ENABLED = "true"
$env:WAYANG_PROFILE_PARQUET_TARGET = "trino"
$env:WAYANG_PROFILE_PARQUET_EXPECTED_ROWS = "12"
$env:WAYANG_PROFILE_PARQUET_ORDERS_URI = "gs://YOUR_BUCKET/wayang-profile/orders.parquet"
$env:WAYANG_PROFILE_PARQUET_CUSTOMERS_URI = "gs://YOUR_BUCKET/wayang-profile/customers.parquet"

$env:WAYANG_PROFILE_PARQUET_TRINO_ORDERS_RELATION = "iceberg.profile.orders_ext"
$env:WAYANG_PROFILE_PARQUET_TRINO_CUSTOMERS_RELATION = "iceberg.profile.customers_ext"
$env:WAYANG_PROFILE_PARQUET_TRINO_SINK_RELATION = "iceberg.profile.join_profile_out"

$env:WAYANG_TRINO_JDBC_URL = "jdbc:trino://localhost:8080"
$env:WAYANG_TRINO_JDBC_USER = "admin"
$env:WAYANG_TRINO_JDBC_PASSWORD = ""

.\mvnw.cmd -pl wayang-tests-integration `
  "-Dtest=SqlParquetJoinProfilingIT" `
  "-Dsurefire.failIfNoSpecifiedTests=false" `
  "-Drat.skip=true" "-Dlicense.skip=true" `
  -Pskip-prerequisite-check test
```

For Presto or BigQuery, change `WAYANG_PROFILE_PARQUET_TARGET` and set the
matching relation variables:

```text
WAYANG_PROFILE_PARQUET_PRESTO_ORDERS_RELATION
WAYANG_PROFILE_PARQUET_PRESTO_CUSTOMERS_RELATION
WAYANG_PROFILE_PARQUET_PRESTO_SINK_RELATION

WAYANG_PROFILE_PARQUET_BIGQUERY_ORDERS_RELATION
WAYANG_PROFILE_PARQUET_BIGQUERY_CUSTOMERS_RELATION
WAYANG_PROFILE_PARQUET_BIGQUERY_SINK_RELATION
```

The test writes profiling logs under:

```text
target/cost-profiling/parquet-sql/executions.json
target/cost-profiling/parquet-sql/cardinalities.json
target/cost-profiling/parquet-sql/manifest.csv
```

## Local Shared Parquet With Trino And Presto

For local validation without a cloud bucket, use MinIO as shared object storage
and Hive Metastore as the shared catalog. This proves that Trino and Presto read
the same external Parquet files instead of using platform-local memory tables.

Start the Trino shared-storage stack and the Presto stack, upload the Parquet
files, and create the Hive external tables:

```powershell
.\tools\parquet-profile\prepare-sql-shared-parquet-tables.ps1 -StartContainers
```

The script prints:

```text
orders_uri=s3a://warehouse/wayang-profile/orders/
customers_uri=s3a://warehouse/wayang-profile/customers/
orders_relation=hive.wayang_profile.orders_ext
customers_relation=hive.wayang_profile.customers_ext
trino_sink_relation=hive.wayang_profile.join_profile_out
presto_sink_relation=hive.wayang_profile.join_profile_out_presto
expected_join_rows=12
```

Run the Trino target:

```powershell
$env:MAVEN_OPTS = "-Xmx384m -XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseSerialGC -Xss512k"

.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform trino `
  -OrdersUri "s3a://warehouse/wayang-profile/orders/" `
  -CustomersUri "s3a://warehouse/wayang-profile/customers/" `
  -OrdersRelation "hive.wayang_profile.orders_ext" `
  -CustomersRelation "hive.wayang_profile.customers_ext" `
  -SinkRelation "hive.wayang_profile.join_profile_out" `
  -InProcessTests
```

Run the Presto target over the same Parquet-backed Hive tables:

```powershell
$env:MAVEN_OPTS = "-Xmx384m -XX:MaxMetaspaceSize=256m -XX:ReservedCodeCacheSize=64m -XX:+UseSerialGC -Xss512k"

.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform presto `
  -OrdersUri "s3a://warehouse/wayang-profile/orders/" `
  -CustomersUri "s3a://warehouse/wayang-profile/customers/" `
  -OrdersRelation "hive.wayang_profile.orders_ext" `
  -CustomersRelation "hive.wayang_profile.customers_ext" `
  -SinkRelation "hive.wayang_profile.join_profile_out_presto" `
  -InProcessTests
```

BigQuery cannot read a local MinIO bucket, so the true three-engine shared
external-Parquet validation still needs a cloud bucket that BigQuery can access,
such as GCS. The local MinIO path validates the shared-storage design for Trino
and Presto and keeps the same Wayang `ParquetSource` mapping flow.

## BigQuery Fallback Without A GCS Bucket

If the GCP project cannot create a GCS bucket, the external-table flow above
cannot be completed. As a temporary smoke test, load the generated Parquet files
into native BigQuery tables and map the logical `ParquetSource` URIs to those
tables:

```powershell
.\tools\parquet-profile\prepare-bigquery-native-parquet-tables.ps1 `
  -ProjectId "hw2-project-487519" `
  -Dataset "wayang_profile" `
  -HttpProxy "http://127.0.0.1:7890"
```

Then run the profiling IT against the loaded tables:

```powershell
$env:HTTP_PROXY = "http://127.0.0.1:7890"
$env:HTTPS_PROXY = "http://127.0.0.1:7890"
$env:JAVA_TOOL_OPTIONS = "-Dhttp.proxyHost=127.0.0.1 -Dhttp.proxyPort=7890 -Dhttps.proxyHost=127.0.0.1 -Dhttps.proxyPort=7890"
$env:WAYANG_BIGQUERY_JDBC_URL = "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;ProjectId=hw2-project-487519;OAuthType=0;OAuthServiceAcctEmail=wayang-bq@hw2-project-487519.iam.gserviceaccount.com;OAuthPvtKeyPath=C:/Users/jizhi/wayang-bq-key.json;Location=US"

.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform bigquery `
  -OrdersUri "file:///C:/wayang/target/shared-parquet-profile/orders.parquet" `
  -CustomersUri "file:///C:/wayang/target/shared-parquet-profile/customers.parquet" `
  -OrdersRelation "`hw2-project-487519.wayang_profile.orders_parquet_native`" `
  -CustomersRelation "`hw2-project-487519.wayang_profile.customers_parquet_native`" `
  -SinkRelation "`hw2-project-487519.wayang_profile.join_profile_out`"
```

This verifies the Wayang `ParquetSource` mapping and BigQuery SQL join path, but
it is not the final shared external-Parquet validation because BigQuery reads
native tables after the load job. Use the GCS external-table flow when a bucket
is available.

## Trino And Presto Fallback Without Shared Storage

If no shared GCS/S3 bucket is available, Trino and Presto can still validate the
Wayang `ParquetSource` mapping and SQL join execution path with their memory
connectors. This does not prove that Trino/Presto read external Parquet files;
it is a smoke test for the Wayang SQL path.

Start and prepare Trino:

```powershell
.\tools\parquet-profile\prepare-sql-memory-parquet-tables.ps1 `
  -TargetPlatform trino `
  -StartContainer
```

Run the Trino profiling target:

```powershell
$env:WAYANG_TRINO_JDBC_URL = "jdbc:trino://localhost:8080"
$env:WAYANG_TRINO_JDBC_USER = "admin"
$env:WAYANG_TRINO_JDBC_PASSWORD = ""

.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform trino `
  -OrdersUri "file:///C:/wayang/target/shared-parquet-profile/orders.parquet" `
  -CustomersUri "file:///C:/wayang/target/shared-parquet-profile/customers.parquet" `
  -OrdersRelation "memory.wayang_profile.orders_parquet_native" `
  -CustomersRelation "memory.wayang_profile.customers_parquet_native" `
  -SinkRelation "memory.wayang_profile.join_profile_out"
```

Start and prepare Presto:

```powershell
.\tools\parquet-profile\prepare-sql-memory-parquet-tables.ps1 `
  -TargetPlatform presto `
  -StartContainer
```

Run the Presto profiling target:

```powershell
$env:WAYANG_PRESTO_JDBC_URL = "jdbc:presto://localhost:8081/memory"
$env:WAYANG_PRESTO_JDBC_USER = "test"

.\tools\parquet-profile\run-sql-parquet-join-profiling.ps1 `
  -TargetPlatform presto `
  -OrdersUri "file:///C:/wayang/target/shared-parquet-profile/orders.parquet" `
  -CustomersUri "file:///C:/wayang/target/shared-parquet-profile/customers.parquet" `
  -OrdersRelation "memory.wayang_profile.orders_parquet_native" `
  -CustomersRelation "memory.wayang_profile.customers_parquet_native" `
  -SinkRelation "memory.wayang_profile.join_profile_out"
```
