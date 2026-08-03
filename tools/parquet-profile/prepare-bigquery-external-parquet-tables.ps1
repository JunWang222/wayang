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
    [string]$ProjectId,
    [string]$Bucket = "",
    [string]$Dataset = "wayang_profile",
    [string]$Location = "US",
    [string]$OutputDir = "target/shared-parquet-profile",
    [string]$GcsPrefix = "wayang-profile",
    [string]$HttpProxy = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$bucketName = if ($Bucket -eq "") { "$ProjectId-wayang-profile" } else { $Bucket }
$normalizedPrefix = $GcsPrefix.Trim("/")
$gcsRoot = "gs://$bucketName/$normalizedPrefix"
$ordersUri = "$gcsRoot/orders.parquet"
$customersUri = "$gcsRoot/customers.parquet"
$ordersDirectoryUri = "$gcsRoot/orders/"
$customersDirectoryUri = "$gcsRoot/customers/"
$ordersDirectoryFileUri = "$gcsRoot/orders/orders.parquet"
$customersDirectoryFileUri = "$gcsRoot/customers/customers.parquet"
$ordersPath = Join-Path $repoRoot "$OutputDir\orders.parquet"
$customersPath = Join-Path $repoRoot "$OutputDir\customers.parquet"
$ordersRelation = "``$ProjectId.$Dataset.orders_ext``"
$customersRelation = "``$ProjectId.$Dataset.customers_ext``"
$sinkRelation = "``$ProjectId.$Dataset.join_profile_out``"

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

function Write-PreparedValues {
    Write-Host "orders_uri=$ordersUri"
    Write-Host "customers_uri=$customersUri"
    Write-Host "orders_directory_uri=$ordersDirectoryUri"
    Write-Host "customers_directory_uri=$customersDirectoryUri"
    Write-Host "orders_relation=$ordersRelation"
    Write-Host "customers_relation=$customersRelation"
    Write-Host "sink_relation=$sinkRelation"
    Write-Host "expected_join_rows=12"
}

if ($HttpProxy -ne "") {
    $env:HTTP_PROXY = $HttpProxy
    $env:HTTPS_PROXY = $HttpProxy
}

Push-Location $repoRoot
try {
    if ($DryRun) {
        Write-Host "Dry run only. Would prepare BigQuery external Parquet tables:"
        Write-PreparedValues
        exit 0
    }

    .\tools\parquet-profile\generate-shared-parquet-data.ps1 -OutputDir $OutputDir

    gsutil ls -b "gs://$bucketName" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Invoke-CheckedCommand {
            gsutil mb -p $ProjectId -l $Location "gs://$bucketName"
        } "Creating GCS bucket"
    }

    Invoke-CheckedCommand {
        gsutil cp $ordersPath $ordersUri
    } "Uploading orders Parquet to GCS"
    Invoke-CheckedCommand {
        gsutil cp $customersPath $customersUri
    } "Uploading customers Parquet to GCS"
    Invoke-CheckedCommand {
        gsutil cp $ordersPath $ordersDirectoryFileUri
    } "Uploading Hive-friendly orders Parquet directory to GCS"
    Invoke-CheckedCommand {
        gsutil cp $customersPath $customersDirectoryFileUri
    } "Uploading Hive-friendly customers Parquet directory to GCS"

    bq --project_id=$ProjectId --location=$Location mk --dataset $Dataset 2>$null

    $ordersSql = "CREATE OR REPLACE EXTERNAL TABLE $ordersRelation (order_id INT64, customer_id INT64, region STRING, amount FLOAT64, bucket INT64) OPTIONS (format = 'PARQUET', uris = ['$ordersUri']);"
    $customersSql = "CREATE OR REPLACE EXTERNAL TABLE $customersRelation (cust_id INT64, tier STRING) OPTIONS (format = 'PARQUET', uris = ['$customersUri']);"

    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location query --use_legacy_sql=false $ordersSql
    } "Creating BigQuery orders external table"
    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location query --use_legacy_sql=false $customersSql
    } "Creating BigQuery customers external table"
    Invoke-CheckedCommand {
        bq --project_id=$ProjectId --location=$Location query --use_legacy_sql=false `
            "SELECT (SELECT COUNT(*) FROM $ordersRelation) AS orders_rows, (SELECT COUNT(*) FROM $customersRelation) AS customers_rows"
    } "Checking BigQuery external table row counts"

    Write-PreparedValues
} finally {
    Pop-Location
}
