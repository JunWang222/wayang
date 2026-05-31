/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.genericjdbc;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.function.ProjectionDescriptor;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.genericjdbc.presto.PrestoFilterOperator;
import org.apache.wayang.genericjdbc.presto.PrestoPlatform;
import org.apache.wayang.genericjdbc.presto.PrestoTableSource;
import org.apache.wayang.java.Java;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PrestoGenericJdbcIT {

    private static final String PRESTO_HOST = System.getenv().getOrDefault("PRESTO_HOST", "localhost");
    private static final int PRESTO_PORT = Integer.parseInt(System.getenv().getOrDefault("PRESTO_PORT", "8080"));
    private static final String JDBC_SERVER_URL = String.format("jdbc:presto://%s:%d", PRESTO_HOST, PRESTO_PORT);
    private static final String JDBC_URL = String.format("jdbc:presto://%s:%d/memory/sales", PRESTO_HOST, PRESTO_PORT);

    private static boolean prestoAvailable = false;

    @BeforeAll
    static void checkPrestoAvailableAndLoadData() {
        try {
            Properties props = new Properties();
            props.setProperty("user", "admin");
            try (Connection conn = DriverManager.getConnection(JDBC_SERVER_URL, props);
                 Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE SCHEMA IF NOT EXISTS memory.sales");
                stmt.execute("DROP TABLE IF EXISTS memory.sales.orders");
                stmt.execute(
                        "CREATE TABLE memory.sales.orders (" +
                                "order_id INTEGER, " +
                                "region VARCHAR, " +
                                "product VARCHAR, " +
                                "amount DOUBLE, " +
                                "order_date DATE)"
                );
                stmt.execute(
                        "INSERT INTO memory.sales.orders VALUES " +
                                "(1, 'APAC', 'laptop', 1200.50, DATE '2026-01-02')," +
                                "(2, 'EMEA', 'phone', 850.00, DATE '2026-01-03')," +
                                "(3, 'AMER', 'tablet', 640.25, DATE '2026-01-04')," +
                                "(4, 'APAC', 'monitor', 310.00, DATE '2026-01-05')," +
                                "(5, 'EMEA', 'keyboard', 95.00, DATE '2026-01-06')," +
                                "(6, 'AMER', 'server', 2450.00, DATE '2026-01-07')," +
                                "(7, 'APAC', 'mouse', 35.00, DATE '2026-01-08')," +
                                "(8, 'EMEA', 'router', 420.00, DATE '2026-01-09')," +
                                "(9, 'AMER', 'switch', 780.00, DATE '2026-01-10')," +
                                "(10, 'APAC', 'storage', 1625.00, DATE '2026-01-11')," +
                                "(11, 'EMEA', 'laptop', 1320.00, DATE '2026-01-12')," +
                                "(12, 'AMER', 'phone', 910.00, DATE '2026-01-13')," +
                                "(13, 'APAC', 'tablet', 540.00, DATE '2026-01-14')," +
                                "(14, 'EMEA', 'monitor', 275.00, DATE '2026-01-15')," +
                                "(15, 'AMER', 'keyboard', 105.00, DATE '2026-01-16')," +
                                "(16, 'APAC', 'server', 3100.00, DATE '2026-01-17')," +
                                "(17, 'EMEA', 'mouse', 42.00, DATE '2026-01-18')," +
                                "(18, 'AMER', 'router', 465.00, DATE '2026-01-19')," +
                                "(19, 'APAC', 'switch', 805.00, DATE '2026-01-20')," +
                                "(20, 'EMEA', 'storage', 1710.00, DATE '2026-01-21')"
                );
                prestoAvailable = true;
            }
        } catch (Exception e) {
            System.err.println("[SETUP] Presto not available; tests will be skipped: " + e.getMessage());
        }
    }

    private Configuration createPrestoConfig() {
        Configuration config = new Configuration();
        config.setProperty("wayang.presto.jdbc.url", JDBC_URL);
        config.setProperty("wayang.presto.jdbc.user", "admin");
        config.setProperty("wayang.presto.jdbc.password", "");
        return config;
    }

    private WayangContext createContext(Configuration config) {
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Presto.plugin());
    }

    private List<String> executeDirectSql(String sql) throws Exception {
        List<String> rows = new ArrayList<>();
        Properties props = new Properties();
        props.setProperty("user", "admin");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, props);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int width = metaData.getColumnCount();
            while (rs.next()) {
                List<String> fields = new ArrayList<>(width);
                for (int i = 1; i <= width; i++) {
                    fields.add(Objects.toString(rs.getObject(i), "null"));
                }
                rows.add(String.join(" | ", fields));
            }
        }
        return rows;
    }

    private List<String> formatRecords(List<Record> records) {
        List<String> rows = new ArrayList<>(records.size());
        for (Record record : records) {
            List<String> fields = new ArrayList<>(record.size());
            for (int i = 0; i < record.size(); i++) {
                fields.add(Objects.toString(record.getField(i), "null"));
            }
            rows.add(String.join(" | ", fields));
        }
        return rows;
    }

    private void printAndAssertSameResults(
            String label,
            String directSql,
            List<String> directRows,
            List<Record> wayangRecords) {
        List<String> wayangRows = this.formatRecords(wayangRecords);
        List<String> sortedDirectRows = new ArrayList<>(directRows);
        List<String> sortedWayangRows = new ArrayList<>(wayangRows);
        Collections.sort(sortedDirectRows);
        Collections.sort(sortedWayangRows);

        System.out.println();
        System.out.println("============================================================");
        System.out.println("[PRESTO VERIFY] " + label);
        System.out.println("Direct Presto JDBC SQL:");
        System.out.println("  " + directSql);
        System.out.println("Direct Presto JDBC result (" + directRows.size() + " rows):");
        directRows.forEach(row -> System.out.println("  " + row));
        System.out.println("Wayang through Presto result (" + wayangRows.size() + " rows):");
        wayangRows.forEach(row -> System.out.println("  " + row));
        System.out.println("Comparison:");
        System.out.println("  sorted direct JDBC rows == sorted Wayang rows");
        System.out.println("============================================================");
        System.out.println();

        assertEquals(sortedDirectRows, sortedWayangRows,
                "Direct Presto JDBC and Wayang-through-Presto results must match.");
    }

    private long countPrestoHistoryEntries(String exactQuery) throws Exception {
        String escapedQuery = exactQuery.replace("'", "''");
        String historySql = "SELECT count(*) FROM system.runtime.queries WHERE query = '" + escapedQuery + "'";
        List<String> rows = this.executeDirectSql(historySql);
        assertEquals(1, rows.size(), "Presto query history count should return one row.");
        return Long.parseLong(rows.get(0));
    }

    @Test
    @Order(0)
    @DisplayName("Presto plugin exposes platform, mappings, and channel conversions")
    void testPluginContributions() {
        org.apache.wayang.core.plugin.Plugin plugin = Presto.plugin();

        assertFalse(plugin.getRequiredPlatforms().isEmpty());
        assertFalse(plugin.getMappings().isEmpty());
        assertFalse(plugin.getChannelConversions().isEmpty());
        assertTrue(plugin.getRequiredPlatforms().stream()
                .anyMatch(p -> "presto".equals(p.getConfigurationName())));
    }

    @Test
    @Order(1)
    @DisplayName("Presto filter mapping binds physical operator to PrestoPlatform")
    void testPlatformBinding() {
        FilterOperator<Record> logicalFilter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "APAC".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'APAC'")
        );
        PrestoFilterOperator physicalFilter = new PrestoFilterOperator(logicalFilter);

        assertSame(PrestoPlatform.getInstance(), physicalFilter.getPlatform());
        assertEquals("presto", physicalFilter.getPlatform().getPlatformId());
        assertNotEquals(logicalFilter.getClass(), physicalFilter.getClass());
    }

    @Test
    @Order(2)
    @DisplayName("Presto local dataset is reachable through raw JDBC")
    void testRawJdbcDataset() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        String sql = "SELECT count(*) FROM orders";
        List<String> rows = this.executeDirectSql(sql);

        System.out.println();
        System.out.println("============================================================");
        System.out.println("[PRESTO VERIFY] Raw JDBC dataset check");
        System.out.println("Direct Presto JDBC SQL:");
        System.out.println("  " + sql);
        System.out.println("Direct Presto JDBC result:");
        rows.forEach(row -> System.out.println("  " + row));
        System.out.println("============================================================");
        System.out.println();

        assertEquals(Collections.singletonList("20"), rows, "Expected 20 rows in the local Presto dataset");
    }

    @Test
    @Order(3)
    @DisplayName("Presto: Wayang-submitted SQL appears in Presto query history")
    void testWayangQueryAppearsInPrestoHistory() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        String expectedWayangSql = "SELECT * FROM orders WHERE product = 'server' AND amount > 2000";
        long beforeCount = this.countPrestoHistoryEntries(expectedWayangSql);

        List<Record> results = new ArrayList<>();
        PrestoTableSource source = new PrestoTableSource(
                "orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> "server".equals(record.getField(2)) &&
                                ((Number) record.getField(3)).doubleValue() > 2000.0,
                        Record.class
                ).withSqlImplementation("product = 'server' AND amount > 2000")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createPrestoConfig()).execute("Presto-VerifyHistory", new WayangPlan(sink));

        long afterCount = this.countPrestoHistoryEntries(expectedWayangSql);

        System.out.println();
        System.out.println("============================================================");
        System.out.println("[PRESTO VERIFY] Wayang SQL appears in Presto query history");
        System.out.println("Expected Wayang-generated SQL:");
        System.out.println("  " + expectedWayangSql);
        System.out.println("Presto history count before Wayang execution: " + beforeCount);
        System.out.println("Presto history count after Wayang execution : " + afterCount);
        System.out.println("Wayang result rows: " + results.size());
        results.forEach(row -> System.out.println("  " + this.formatRecords(Collections.singletonList(row)).get(0)));
        System.out.println("============================================================");
        System.out.println();

        assertTrue(afterCount > beforeCount,
                "Presto system.runtime.queries must record the SQL submitted by Wayang.");
        assertEquals(2, results.size(), "Expected 2 server rows with amount > 2000");
    }

    @Test
    @Order(4)
    @DisplayName("Presto: full table scan")
    void testTableScan() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        List<Record> results = new ArrayList<>();
        PrestoTableSource source = new PrestoTableSource(
                "orders", "order_id", "region", "product", "amount", "order_date"
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        createContext(createPrestoConfig()).execute("Presto-TableScan", new WayangPlan(sink));

        String directSql = "SELECT order_id, region, product, amount, order_date FROM orders";
        List<String> directRows = this.executeDirectSql(directSql);
        this.printAndAssertSameResults("Table scan", directSql, directRows, results);
        assertEquals(20, results.size(), "Expected 20 rows from memory.sales.orders");
    }

    @Test
    @Order(5)
    @DisplayName("Presto: filter pushdown")
    void testFilterPushdown() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        List<Record> results = new ArrayList<>();
        PrestoTableSource source = new PrestoTableSource(
                "orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> "APAC".equals(record.getField(1)),
                        Record.class
                ).withSqlImplementation("region = 'APAC'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createPrestoConfig()).execute("Presto-Filter", new WayangPlan(sink));

        String directSql = "SELECT order_id, region, product, amount, order_date FROM orders WHERE region = 'APAC'";
        List<String> directRows = this.executeDirectSql(directSql);
        this.printAndAssertSameResults("Filter pushdown: region = 'APAC'", directSql, directRows, results);
        assertEquals(7, results.size(), "Expected 7 APAC rows");
        results.forEach(r -> assertEquals("APAC", r.getField(1)));
    }

    @Test
    @Order(6)
    @DisplayName("Presto: projection pushdown")
    void testProjectionPushdown() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        List<Record> results = new ArrayList<>();
        PrestoTableSource source = new PrestoTableSource(
                "orders", "order_id", "region", "product", "amount", "order_date"
        );
        MapOperator<Record, Record> project = new MapOperator<>(
                new ProjectionDescriptor<>(Record.class, Record.class, "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, project, 0);
        project.connectTo(0, sink, 0);

        createContext(createPrestoConfig()).execute("Presto-Projection", new WayangPlan(sink));

        String directSql = "SELECT region, amount FROM orders";
        List<String> directRows = this.executeDirectSql(directSql);
        this.printAndAssertSameResults("Projection pushdown: region, amount", directSql, directRows, results);
        assertEquals(20, results.size(), "Expected 20 projected rows");
        results.forEach(r -> assertEquals(2, r.size(), "Record should contain only projected fields"));
    }

    @Test
    @Order(7)
    @DisplayName("Presto: filter and projection pipeline")
    void testFilterThenProjection() throws Exception {
        Assumptions.assumeTrue(prestoAvailable, "Presto not available");

        List<Record> results = new ArrayList<>();
        PrestoTableSource source = new PrestoTableSource(
                "orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> ((Number) record.getField(3)).doubleValue() > 1000.0,
                        Record.class
                ).withSqlImplementation("amount > 1000")
        );
        MapOperator<Record, Record> project = new MapOperator<>(
                new ProjectionDescriptor<>(Record.class, Record.class, "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, project, 0);
        project.connectTo(0, sink, 0);

        createContext(createPrestoConfig()).execute("Presto-Filter-Projection", new WayangPlan(sink));

        String directSql = "SELECT region, amount FROM orders WHERE amount > 1000";
        List<String> directRows = this.executeDirectSql(directSql);
        this.printAndAssertSameResults("Filter + projection: amount > 1000", directSql, directRows, results);
        assertEquals(6, results.size(), "Expected 6 rows with amount > 1000");
        results.forEach(r -> assertTrue(((Number) r.getField(1)).doubleValue() > 1000.0));
    }
}
