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
    [ValidateSet("trino", "presto")]
    [string]$TargetPlatform = "trino",
    [switch]$StartContainer
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$schema = "memory.wayang_profile"
$ordersRelation = "$schema.orders_parquet_native"
$customersRelation = "$schema.customers_parquet_native"
$sinkRelation = "$schema.join_profile_out"
$sqlPath = Join-Path $repoRoot "target\$TargetPlatform-profile-setup.sql"
$containerName = $TargetPlatform

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

$sql = @"
CREATE SCHEMA IF NOT EXISTS $schema;
DROP TABLE IF EXISTS $sinkRelation;
DROP TABLE IF EXISTS $ordersRelation;
DROP TABLE IF EXISTS $customersRelation;
CREATE TABLE $ordersRelation (order_id BIGINT, customer_id BIGINT, region VARCHAR, amount DOUBLE, bucket BIGINT);
INSERT INTO $ordersRelation VALUES
(1001, 1, 'north', 42.5, 0),
(1002, 2, 'south', 18.0, 0),
(1003, 3, 'west', 23.5, 1),
(1004, 1, 'north', 99.0, 1),
(1005, 4, 'east', 14.0, 2),
(1006, 5, 'south', 67.2, 2),
(1007, 2, 'south', 31.8, 3),
(1008, 3, 'west', 44.4, 3),
(1009, 4, 'east', 52.1, 4),
(1010, 5, 'north', 73.3, 4),
(1011, 1, 'north', 11.2, 5),
(1012, 2, 'south', 87.6, 5);
CREATE TABLE $customersRelation (cust_id BIGINT, tier VARCHAR);
INSERT INTO $customersRelation VALUES
(1, 'gold'),
(2, 'silver'),
(3, 'bronze'),
(4, 'gold'),
(5, 'silver');
SELECT (SELECT count(*) FROM $ordersRelation) AS orders_rows, (SELECT count(*) FROM $customersRelation) AS customers_rows;
"@

Push-Location $repoRoot
try {
    if ($StartContainer) {
        Invoke-CheckedCommand {
            docker compose -f "$TargetPlatform-setup\docker-compose.yml" up -d --wait
        } "Starting $TargetPlatform"
    }

    New-Item -ItemType Directory -Force (Split-Path $sqlPath) | Out-Null
    Set-Content -LiteralPath $sqlPath -Value $sql -Encoding ASCII
    Invoke-CheckedCommand {
        docker cp $sqlPath "${containerName}:/tmp/sql-profile-setup.sql"
    } "Copying setup SQL into $containerName"

    if ($TargetPlatform -eq "trino") {
        Invoke-CheckedCommand {
            docker exec $containerName trino --server localhost:8080 --catalog memory --schema wayang_profile --file /tmp/sql-profile-setup.sql
        } "Preparing Trino memory tables"
    } else {
        Invoke-CheckedCommand {
            docker exec $containerName /opt/presto-cli --server localhost:8080 --catalog memory --schema wayang_profile --file /tmp/sql-profile-setup.sql
        } "Preparing Presto memory tables"
    }

    Write-Host "orders_relation=$ordersRelation"
    Write-Host "customers_relation=$customersRelation"
    Write-Host "sink_relation=$sinkRelation"
    Write-Host "expected_join_rows=12"
} finally {
    Pop-Location
}
