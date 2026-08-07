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
    [Parameter(Mandatory = $true)]
    [string]$OrdersUri,
    [Parameter(Mandatory = $true)]
    [string]$CustomersUri,
    [string]$ProjectId = "",
    [string]$Dataset = "wayang_profile_gcs",
    [string]$OutputDir = "target/cost-profiling/parquet-sql-partitioned",
    [string]$TrinoOrdersRelation = "hivegcs.wayang_profile_gcs.orders_part_trino",
    [string]$TrinoCustomersRelation = "hivegcs.wayang_profile_gcs.customers_gcs_ext",
    [string]$TrinoSinkRelation = "hivegcs.wayang_profile_gcs.join_profile_part_trino",
    [string]$PrestoOrdersRelation = "hivegcs.wayang_profile_gcs.orders_part_presto",
    [string]$PrestoCustomersRelation = "hivegcs.wayang_profile_gcs.customers_gcs_ext_presto",
    [string]$PrestoSinkRelation = "hivegcs.wayang_profile_gcs.join_profile_part_presto",
    [string]$BigQueryOrdersRelation = "",
    [string]$BigQueryCustomersRelation = "",
    [string]$BigQuerySinkRelation = "",
    [string]$TrinoExpectedRows = "4",
    [string]$PrestoExpectedRows = "4",
    [string]$BigQueryExpectedRows = "4",
    [switch]$CleanupSinks,
    [switch]$DryRun,
    [switch]$InProcessTests
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$singleRunner = Join-Path $PSScriptRoot "run-sql-parquet-join-profiling.ps1"

if ($ProjectId -eq "" -and ($BigQueryOrdersRelation -eq "" -or $BigQueryCustomersRelation -eq "" -or $BigQuerySinkRelation -eq "")) {
    throw "Set -ProjectId or provide all BigQuery relation parameters."
}

if ($BigQueryOrdersRelation -eq "") {
    $BigQueryOrdersRelation = "``$ProjectId.$Dataset.orders_part_bq``"
}
if ($BigQueryCustomersRelation -eq "") {
    $BigQueryCustomersRelation = "``$ProjectId.$Dataset.customers_gcs_ext_bq``"
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

function Invoke-PartitionRun {
    param(
        [string]$TargetPlatform,
        [string]$OrdersRelation,
        [string]$CustomersRelation,
        [string]$SinkRelation,
        [string]$ExpectedRows,
        [string]$BucketPredicate
    )

    Write-Host ""
    Write-Host "Running partition: platform=$TargetPlatform, $BucketPredicate, expected_rows=$ExpectedRows"

    $runnerArgs = @{
        TargetPlatform = $TargetPlatform
        OrdersUri = $OrdersUri
        CustomersUri = $CustomersUri
        OrdersRelation = $OrdersRelation
        CustomersRelation = $CustomersRelation
        SinkRelation = $SinkRelation
        ExpectedRows = $ExpectedRows
    }
    if ($DryRun) {
        $runnerArgs["DryRun"] = $true
    }
    if ($InProcessTests) {
        $runnerArgs["InProcessTests"] = $true
    }

    Invoke-CheckedCommand {
        & $singleRunner @runnerArgs
    } "Running $TargetPlatform partition"
}

Push-Location $repoRoot
try {
    $env:WAYANG_PROFILE_PARQUET_OUTPUT_DIR = $OutputDir
    $env:WAYANG_PROFILE_PARQUET_CLEANUP = if ($CleanupSinks) { "true" } else { "false" }

    Write-Host "Logical partitioned query:"
    Write-Host "orders JOIN customers ON orders.customer_id = customers.cust_id"
    Write-Host "orders_uri=$OrdersUri"
    Write-Host "customers_uri=$CustomersUri"
    Write-Host "output_dir=$OutputDir"
    Write-Host "cleanup_sinks=$env:WAYANG_PROFILE_PARQUET_CLEANUP"

    Invoke-PartitionRun "trino" $TrinoOrdersRelation $TrinoCustomersRelation $TrinoSinkRelation `
        $TrinoExpectedRows "orders.bucket IN (0, 1)"
    Invoke-PartitionRun "presto" $PrestoOrdersRelation $PrestoCustomersRelation $PrestoSinkRelation `
        $PrestoExpectedRows "orders.bucket IN (2, 3)"
    Invoke-PartitionRun "bigquery" $BigQueryOrdersRelation $BigQueryCustomersRelation $BigQuerySinkRelation `
        $BigQueryExpectedRows "orders.bucket IN (4, 5)"

    $totalExpectedRows = [long]$TrinoExpectedRows + [long]$PrestoExpectedRows + [long]$BigQueryExpectedRows
    Write-Host ""
    Write-Host "Partitioned join profiling completed."
    Write-Host "total_expected_rows=$totalExpectedRows"
    Write-Host "manifest=$OutputDir/manifest.csv"
} finally {
    Pop-Location
}
