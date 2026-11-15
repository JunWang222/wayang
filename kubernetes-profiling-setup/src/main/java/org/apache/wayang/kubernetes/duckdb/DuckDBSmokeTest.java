/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.kubernetes.duckdb;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.duckdb.DuckDB;
import org.apache.wayang.duckdb.operators.DuckDBTableSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/** Validates an entirely DuckDB-backed Wayang plan in one Kubernetes Pod. */
public final class DuckDBSmokeTest {

    private static final String SCHEMA = "wayang_k8s";
    private static final String SOURCE = SCHEMA + ".orders";
    private static final String SINK = SCHEMA + ".filtered_orders";

    private DuckDBSmokeTest() {
    }

    public static void main(String[] args) throws Exception {
        final long rowCount = parsePositiveLong("ROW_COUNT", 10_000L);
        final Path databasePath = Path.of(
                System.getenv().getOrDefault(
                        "DUCKDB_DATABASE_PATH", "/tmp/wayang-duckdb/wayang.duckdb"))
                .toAbsolutePath();
        Files.createDirectories(databasePath.getParent());
        final String jdbcUrl = "jdbc:duckdb:" + databasePath;

        Class.forName("org.duckdb.DuckDBDriver");
        prepareFixture(jdbcUrl, rowCount);

        final Configuration configuration = new Configuration();
        configuration.setProperty("wayang.duckdb.jdbc.url", jdbcUrl);
        configuration.setProperty("wayang.duckdb.jdbc.user", "");
        configuration.setProperty("wayang.duckdb.jdbc.password", "");

        final DuckDBTableSource source = new DuckDBTableSource(
                SOURCE, "order_id", "customer_id", "region", "amount");
        final FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> "AMER".equals(record.getField(2)), Record.class)
                        .withSqlImplementation("region = 'AMER'"));
        final TableSink<Record> sink = new TableSink<>(
                new Properties(), "overwrite", SINK,
                "order_id", "customer_id", "region", "amount");
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        final long startedAt = System.nanoTime();
        new WayangContext(configuration)
                .withPlugin(DuckDB.plugin())
                .execute(new WayangPlan(sink));
        final long wallClockMillis = (System.nanoTime() - startedAt) / 1_000_000L;

        final long outputCount = queryCount(jdbcUrl);
        final long expectedCount = (rowCount + 2L) / 3L;
        if (outputCount != expectedCount) {
            throw new IllegalStateException(
                    "Expected " + expectedCount + " rows but found " + outputCount + ".");
        }

        System.out.printf(
                "WAYANG_K8S_DUCKDB_RESULT inputRows=%d outputRows=%d wallClockMs=%d "
                        + "database=%s status=PASS%n",
                rowCount, outputCount, wallClockMillis, databasePath);
    }

    private static void prepareFixture(String jdbcUrl, long rowCount) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + SINK);
            statement.execute("DROP TABLE IF EXISTS " + SOURCE);
            statement.execute(
                    "CREATE TABLE " + SOURCE + " AS "
                            + "SELECT i AS order_id, i % 1000 AS customer_id, "
                            + "CASE WHEN i % 3 = 0 THEN 'AMER' "
                            + "WHEN i % 3 = 1 THEN 'EMEA' ELSE 'APAC' END AS region, "
                            + "CAST(i AS DOUBLE) * 1.25 AS amount "
                            + "FROM range(0, " + rowCount + ") AS rows(i)");
        }
    }

    private static long queryCount(String jdbcUrl) throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT count(*) FROM " + SINK)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private static long parsePositiveLong(String name, long defaultValue) {
        final String value = System.getenv(name);
        final long parsed = value == null || value.isBlank() ? defaultValue : Long.parseLong(value);
        if (parsed <= 0L) {
            throw new IllegalArgumentException(name + " must be positive.");
        }
        return parsed;
    }
}
