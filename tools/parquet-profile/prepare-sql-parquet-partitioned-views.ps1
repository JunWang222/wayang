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
    [string]$ProjectId = "",
    [string]$Dataset = "wayang_profile_gcs",
    [string]$Location = "US",
    [string]$TrinoContainer = "trino",
    [string]$PrestoContainer = "presto",
    [string]$TrinoOrdersSourceRelation = "hivegcs.wayang_profile_gcs.orders_gcs_ext",
    [string]$TrinoCustomersRelation = "hivegcs.wayang_profile_gcs.customers_gcs_ext",
    [string]$TrinoOrdersPartitionRelation = "hivegcs.wayang_profile_gcs.orders_part_trino",
    [string]$TrinoSinkRelation = "hivegcs.wayang_profile_gcs.join_profile_part_trino",
    [string]$PrestoOrdersSourceRelation = "hivegcs.wayang_profile_gcs.orders_gcs_ext_presto",
    [string]$PrestoCustomersRelation = "hivegcs.wayang_profile_gcs.customers_gcs_ext_presto",
    [string]$PrestoOrdersPartitionRelation = "hivegcs.wayang_profile_gcs.orders_part_presto",
    [string]$PrestoSinkRelation = "hivegcs.wayang_profile_gcs.join_profile_part_presto",
    [string]$BigQueryOrdersSourceRelation = "",
    [string]$BigQueryCustomersRelation = "",
    [string]$BigQueryOrdersPartitionRelation = "",
    [string]$BigQuerySinkRelation = "",
    [switch]$SkipTrino,
    [switch]$SkipPresto,
    [switch]$SkipBigQuery,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

if (-not $SkipBigQuery -and $ProjectId -eq "") {
    throw "Set -ProjectId or provide all BigQuery relation parameters."
}

if ($BigQueryOrdersSourceRelation -eq "") {
    $BigQueryOrdersSourceRelation = "``$ProjectId.$Dataset.orders_gcs_ext_bq``"
}
if ($BigQueryCustomersRelation -eq "") {
    $BigQueryCustomersRelation = "``$ProjectId.$Dataset.customers_gcs_ext_bq``"
}
if ($BigQueryOrdersPartitionRelation -eq "") {
    $BigQueryOrdersPartitionRelation = "``$ProjectId.$Dataset.orders_part_bq``"
}
if ($BigQuerySinkRelation -eq "") {
    $BigQuerySinkRelation = "``$ProjectId.$Dataset.join_profile_part_bq``"
}

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

function Invoke-Sql {
    param(
        [ScriptBlock]$Command,
        [string]$Sql,
        [string]$Description
    )

    Write-Host ""
    Write-Host $Description
    if ($DryRun) {
        Write-Host $Sql
        return
    }
    Invoke-CheckedCommand $Command $Description
}

function Write-PreparedValues {
    Write-Host ""
    Write-Host "Partitioned profiling relations:"
    Write-Host "trino_orders_relation=$TrinoOrdersPartitionRelation"
    Write-Host "trino_customers_relation=$TrinoCustomersRelation"
    Write-Host "trino_sink_relation=$TrinoSinkRelation"
    Write-Host "trino_bucket_predicate=bucket IN (0, 1)"
    Write-Host "trino_expected_rows=4"
    Write-Host "presto_orders_relation=$PrestoOrdersPartitionRelation"
    Write-Host "presto_customers_relation=$PrestoCustomersRelation"
    Write-Host "presto_sink_relation=$PrestoSinkRelation"
    Write-Host "presto_bucket_predicate=bucket IN (2, 3)"
    Write-Host "presto_expected_rows=4"
    Write-Host "bigquery_orders_relation=$BigQueryOrdersPartitionRelation"
    Write-Host "bigquery_customers_relation=$BigQueryCustomersRelation"
    Write-Host "bigquery_sink_relation=$BigQuerySinkRelation"
    Write-Host "bigquery_bucket_predicate=bucket IN (4, 5)"
    Write-Host "bigquery_expected_rows=4"
    Write-Host "total_expected_rows=12"
}

$trinoSql = @"
DROP VIEW IF EXISTS $TrinoOrdersPartitionRelation;
CREATE VIEW $TrinoOrdersPartitionRelation AS
SELECT * FROM $TrinoOrdersSourceRelation
WHERE bucket IN (0, 1);
SELECT count(*) AS trino_orders_partition_rows FROM $TrinoOrdersPartitionRelation;
"@

$prestoSql = @"
DROP VIEW IF EXISTS $PrestoOrdersPartitionRelation;
CREATE VIEW $PrestoOrdersPartitionRelation AS
SELECT * FROM $PrestoOrdersSourceRelation
WHERE bucket IN (2, 3);
SELECT count(*) AS presto_orders_partition_rows FROM $PrestoOrdersPartitionRelation;
"@

$bigQuerySql = @"
CREATE OR REPLACE VIEW $BigQueryOrdersPartitionRelation AS
SELECT * FROM $BigQueryOrdersSourceRelation
WHERE bucket IN (4, 5);
SELECT count(*) AS bigquery_orders_partition_rows FROM $BigQueryOrdersPartitionRelation;
"@

if (-not $SkipTrino) {
    Invoke-Sql {
        docker exec $TrinoContainer trino --server localhost:8080 --user admin --execute $trinoSql
    } $trinoSql "Preparing Trino partition view"
}

if (-not $SkipPresto) {
    Invoke-Sql {
        docker exec $PrestoContainer /opt/presto-cli --server localhost:8080 --catalog hivegcs --schema wayang_profile_gcs --user test --execute $prestoSql
    } $prestoSql "Preparing Presto partition view"
}

if (-not $SkipBigQuery) {
    Invoke-Sql {
        $sqlPath = Join-Path $repoRoot "target\bigquery-parquet-partitioned-views.sql"
        New-Item -ItemType Directory -Force -Path (Split-Path $sqlPath) | Out-Null
        Set-Content -Path $sqlPath -Value $bigQuerySql -Encoding UTF8
        Get-Content -Path $sqlPath | bq --project_id=$ProjectId --location=$Location query --use_legacy_sql=false
    } $bigQuerySql "Preparing BigQuery partition view"
}

Write-PreparedValues
