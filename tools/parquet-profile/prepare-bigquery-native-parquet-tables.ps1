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
    [string]$ProjectId = "hw2-project-487519",
    [string]$Dataset = "wayang_profile",
    [string]$Location = "US",
    [string]$OutputDir = "target/shared-parquet-profile",
    [string]$HttpProxy = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$ordersTable = "$Dataset.orders_parquet_native"
$customersTable = "$Dataset.customers_parquet_native"
$ordersPath = Join-Path $repoRoot "$OutputDir\orders.parquet"
$customersPath = Join-Path $repoRoot "$OutputDir\customers.parquet"

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

if ($HttpProxy -ne "") {
    $env:HTTP_PROXY = $HttpProxy
    $env:HTTPS_PROXY = $HttpProxy
}

Push-Location $repoRoot
try {
    .\tools\parquet-profile\generate-shared-parquet-data.ps1 -OutputDir $OutputDir

    bq --project_id=$ProjectId --location=$Location mk --dataset $Dataset 2>$null
    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location load --replace --source_format=PARQUET $ordersTable $ordersPath
    } "Loading orders Parquet into BigQuery"
    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location load --replace --source_format=PARQUET $customersTable $customersPath
    } "Loading customers Parquet into BigQuery"

    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location query --use_legacy_sql=false `
            "SELECT (SELECT COUNT(*) FROM ``$ProjectId.$Dataset.orders_parquet_native``) AS orders_rows, (SELECT COUNT(*) FROM ``$ProjectId.$Dataset.customers_parquet_native``) AS customers_rows"
    } "Checking BigQuery Parquet table row counts"

    Write-Host "orders_uri=file:///$($ordersPath.Replace('\', '/'))"
    Write-Host "customers_uri=file:///$($customersPath.Replace('\', '/'))"
    Write-Host "orders_relation=``$ProjectId.$Dataset.orders_parquet_native``"
    Write-Host "customers_relation=``$ProjectId.$Dataset.customers_parquet_native``"
    Write-Host "sink_relation=``$ProjectId.$Dataset.join_profile_out``"
    Write-Host "expected_join_rows=12"
} finally {
    Pop-Location
}
