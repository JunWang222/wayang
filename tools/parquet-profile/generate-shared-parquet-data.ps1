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
    [string]$GcsPrefix = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$classpathFile = Join-Path $repoRoot "wayang-platforms\wayang-java\target\parquet-profile-classpath.txt"
$classesDir = Join-Path $repoRoot "target\parquet-profile-generator\classes"
$sourceFile = Join-Path $PSScriptRoot "GenerateSharedParquetData.java"
$resolvedOutputDir = Join-Path $repoRoot $OutputDir

Push-Location $repoRoot
try {
    New-Item -ItemType Directory -Force $classesDir | Out-Null
    New-Item -ItemType Directory -Force $resolvedOutputDir | Out-Null

    .\mvnw.cmd -pl wayang-platforms/wayang-java dependency:build-classpath `
        "-Dmdep.outputFile=$classpathFile" `
        "-Drat.skip=true" "-Dlicense.skip=true" `
        -Pskip-prerequisite-check

    $classpath = Get-Content $classpathFile
    javac -cp $classpath -d $classesDir $sourceFile

    $generatorOutput = java -cp "$classesDir;$classpath" GenerateSharedParquetData $resolvedOutputDir
    $generatorOutput | ForEach-Object { Write-Host $_ }

    $ordersPath = Join-Path $resolvedOutputDir "orders.parquet"
    $customersPath = Join-Path $resolvedOutputDir "customers.parquet"

    if ($GcsPrefix -ne "") {
        $normalizedPrefix = $GcsPrefix.TrimEnd("/")
        gsutil cp $ordersPath "$normalizedPrefix/orders.parquet"
        gsutil cp $customersPath "$normalizedPrefix/customers.parquet"
        Write-Host "shared_orders_uri=$normalizedPrefix/orders.parquet"
        Write-Host "shared_customers_uri=$normalizedPrefix/customers.parquet"
    }
} finally {
    Pop-Location
}
