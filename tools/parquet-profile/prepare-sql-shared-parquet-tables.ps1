#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

param(
    [string]$OutputDir = "target/shared-parquet-profile",
    [string]$Bucket = "warehouse",
    [string]$Prefix = "wayang-profile",
    [switch]$StartContainers
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$outputPath = Join-Path $repoRoot $OutputDir
$ordersPath = Join-Path $outputPath "orders.parquet"
$customersPath = Join-Path $outputPath "customers.parquet"
$dockerNetwork = "trino-setup_default"
$ordersUri = "s3a://$Bucket/$Prefix/orders/"
$customersUri = "s3a://$Bucket/$Prefix/customers/"
$ordersRelation = "hive.wayang_profile.orders_ext"
$customersRelation = "hive.wayang_profile.customers_ext"
$trinoSinkRelation = "hive.wayang_profile.join_profile_out"
$prestoSinkRelation = "hive.wayang_profile.join_profile_out_presto"

function Invoke-CheckedCommand {
    param(
        [ScriptBlock]$Command,
        [string]$Description
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Description failed with exit code $LASTEXITCODE."
    }
}

Push-Location $repoRoot
try {
    if (-not (Test-Path $ordersPath) -or -not (Test-Path $customersPath)) {
        .\tools\parquet-profile\generate-shared-parquet-data.ps1 -OutputDir $OutputDir
    }
    $resolvedOutputDir = (Resolve-Path $outputPath).Path

    if ($StartContainers) {
        Invoke-CheckedCommand {
            docker compose -f "platforms-setup-guides\trino-setup\docker-compose.yml" up -d --wait
        } "Starting Trino shared-storage stack"
        Invoke-CheckedCommand {
            docker compose -f "presto-setup\docker-compose.yml" up -d --wait
        } "Starting Presto stack"
    }

    Invoke-CheckedCommand {
        docker run --rm --entrypoint /bin/sh --network $dockerNetwork `
            -v "${resolvedOutputDir}:/data:ro" minio/mc:latest `
            -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb local/$Bucket --ignore-existing && mc cp /data/orders.parquet local/$Bucket/$Prefix/orders/orders.parquet && mc cp /data/customers.parquet local/$Bucket/$Prefix/customers/customers.parquet && mc ls --recursive local/$Bucket/$Prefix"
    } "Uploading Parquet files to MinIO"

    Invoke-CheckedCommand {
        docker exec trino trino --server localhost:8080 --user admin --execute `
            "DROP TABLE IF EXISTS $trinoSinkRelation"
    } "Dropping old Trino sink table"

    Invoke-CheckedCommand {
        docker exec presto /opt/presto-cli --server localhost:8080 --catalog hive --schema wayang_profile --user test --execute `
            "DROP TABLE IF EXISTS join_profile_out_presto"
    } "Dropping old Presto sink table"

    $setupSql = @"
CREATE SCHEMA IF NOT EXISTS hive.wayang_profile WITH (location = 's3a://$Bucket/$Prefix/');
DROP TABLE IF EXISTS $ordersRelation;
DROP TABLE IF EXISTS $customersRelation;
CREATE TABLE $ordersRelation (
  order_id integer,
  customer_id integer,
  region varchar,
  amount double,
  bucket integer
)
WITH (
  external_location = '$ordersUri',
  format = 'PARQUET'
);
CREATE TABLE $customersRelation (
  cust_id integer,
  tier varchar
)
WITH (
  external_location = '$customersUri',
  format = 'PARQUET'
);
SELECT (SELECT count(*) FROM $ordersRelation) AS orders_rows, (SELECT count(*) FROM $customersRelation) AS customers_rows;
"@

    Invoke-CheckedCommand {
        docker exec trino trino --server localhost:8080 --execute $setupSql
    } "Creating shared Hive external tables"

    Write-Host "orders_uri=$ordersUri"
    Write-Host "customers_uri=$customersUri"
    Write-Host "orders_relation=$ordersRelation"
    Write-Host "customers_relation=$customersRelation"
    Write-Host "trino_sink_relation=$trinoSinkRelation"
    Write-Host "presto_sink_relation=$prestoSinkRelation"
    Write-Host "expected_join_rows=12"
} finally {
    Pop-Location
}
