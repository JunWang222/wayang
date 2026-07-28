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
    [ValidateSet("trino", "presto", "bigquery")]
    [string]$TargetPlatform = "trino",
    [Parameter(Mandatory = $true)]
    [string]$OrdersUri,
    [Parameter(Mandatory = $true)]
    [string]$CustomersUri,
    [Parameter(Mandatory = $true)]
    [string]$OrdersRelation,
    [Parameter(Mandatory = $true)]
    [string]$CustomersRelation,
    [Parameter(Mandatory = $true)]
    [string]$SinkRelation,
    [string]$ExpectedRows = "12",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$targetPrefix = $TargetPlatform.ToUpperInvariant()

$env:WAYANG_PROFILE_PARQUET_ENABLED = "true"
$env:WAYANG_PROFILE_PARQUET_TARGET = $TargetPlatform
$env:WAYANG_PROFILE_PARQUET_EXPECTED_ROWS = $ExpectedRows
$env:WAYANG_PROFILE_PARQUET_ORDERS_URI = $OrdersUri
$env:WAYANG_PROFILE_PARQUET_CUSTOMERS_URI = $CustomersUri

Set-Item -Path "Env:WAYANG_PROFILE_PARQUET_${targetPrefix}_ORDERS_RELATION" -Value $OrdersRelation
Set-Item -Path "Env:WAYANG_PROFILE_PARQUET_${targetPrefix}_CUSTOMERS_RELATION" -Value $CustomersRelation
Set-Item -Path "Env:WAYANG_PROFILE_PARQUET_${targetPrefix}_SINK_RELATION" -Value $SinkRelation

Write-Host "WAYANG_PROFILE_PARQUET_TARGET=$TargetPlatform"
Write-Host "WAYANG_PROFILE_PARQUET_ORDERS_URI=$OrdersUri"
Write-Host "WAYANG_PROFILE_PARQUET_CUSTOMERS_URI=$CustomersUri"
Write-Host "WAYANG_PROFILE_PARQUET_${targetPrefix}_ORDERS_RELATION=$OrdersRelation"
Write-Host "WAYANG_PROFILE_PARQUET_${targetPrefix}_CUSTOMERS_RELATION=$CustomersRelation"
Write-Host "WAYANG_PROFILE_PARQUET_${targetPrefix}_SINK_RELATION=$SinkRelation"
Write-Host "WAYANG_PROFILE_PARQUET_EXPECTED_ROWS=$ExpectedRows"

if ($DryRun) {
    Write-Host "Dry run only. Re-run without -DryRun to execute SqlParquetJoinProfilingIT."
    exit 0
}

Push-Location $repoRoot
try {
    .\mvnw.cmd -pl wayang-tests-integration `
        "-Dtest=SqlParquetJoinProfilingIT" `
        "-Dsurefire.failIfNoSpecifiedTests=false" `
        "-Drat.skip=true" "-Dlicense.skip=true" `
        -Pskip-prerequisite-check test
} finally {
    Pop-Location
}
