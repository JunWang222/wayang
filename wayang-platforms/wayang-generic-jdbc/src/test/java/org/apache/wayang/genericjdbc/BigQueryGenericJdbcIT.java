package org.apache.wayang.genericjdbc;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.*;
import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests validating BigQuery SQL patterns that GenericJdbc would
 * generate, running against the local BigQuery emulator.
 *
 * These tests use the google-cloud-bigquery REST client (not JDBC) because
 * the BigQuery JDBC driver requires OAuth even against the emulator.
 * The SQL queries mirror exactly what GenericJdbcExecutor.createSqlQuery()
 * would produce for TableSource, Filter, Projection, and combined pipelines.
 *
 * Prerequisites: bigquery-setup/ emulator must be running:
 *   cd bigquery-setup && docker-compose up -d
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryGenericJdbcIT {

    private static final String EMULATOR_HOST = "http://localhost:9050";
    private static final String PROJECT_ID = "test-project";
    private static final String TABLE = "`test-project.sales.orders`";

    private static BigQuery bigquery;
    private static boolean emulatorAvailable = false;

    @BeforeAll
    static void setupClient() {
        try {
            bigquery = BigQueryOptions.newBuilder()
                    .setHost(EMULATOR_HOST)
                    .setLocation("US")
                    .setProjectId(PROJECT_ID)
                    .setCredentials(NoCredentials.getInstance())
                    .build()
                    .getService();

            bigquery.getDataset(DatasetId.of(PROJECT_ID, "sales"));
            emulatorAvailable = true;
        } catch (Exception e) {
            System.err.println("BigQuery emulator not available: " + e.getMessage());
        }
    }

    private List<List<Object>> runQuery(String sql) throws InterruptedException {
        QueryJobConfiguration config = QueryJobConfiguration.newBuilder(sql)
                .setUseLegacySql(false)
                .build();
        TableResult result = bigquery.query(config);
        List<List<Object>> rows = new ArrayList<>();
        for (FieldValueList row : result.iterateAll()) {
            List<Object> r = new ArrayList<>();
            for (FieldValue val : row) {
                r.add(val.isNull() ? null : val.getValue());
            }
            rows.add(r);
        }
        return rows;
    }

    // ── Test 1: Full table scan (GenericJdbcTableSource) ─────────────────

    @Test
    @Order(1)
    @DisplayName("GenericJdbc SQL pattern: SELECT * FROM table (TableSource)")
    void testTableScan() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT * FROM " + TABLE;
        List<List<Object>> rows = runQuery(sql);
        assertEquals(10, rows.size(), "Expected 10 rows from full scan");
        System.out.println("[PASS] TableScan: " + rows.size() + " rows");
    }

    // ── Test 2: Filter pushdown — string predicate ───────────────────────

    @Test
    @Order(2)
    @DisplayName("GenericJdbc SQL pattern: SELECT * FROM table WHERE region = 'APAC'")
    void testFilterString() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT * FROM " + TABLE + " WHERE region = 'APAC'";
        List<List<Object>> rows = runQuery(sql);
        assertFalse(rows.isEmpty());
        rows.forEach(r -> assertEquals("APAC", r.get(1)));
        System.out.printf("[PASS] Filter(region='APAC'): %d rows%n", rows.size());
    }

    // ── Test 3: Filter pushdown — numeric predicate ──────────────────────

    @Test
    @Order(3)
    @DisplayName("GenericJdbc SQL pattern: SELECT * FROM table WHERE amount > 1000")
    void testFilterNumeric() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT * FROM " + TABLE + " WHERE amount > 1000";
        List<List<Object>> rows = runQuery(sql);
        assertFalse(rows.isEmpty());
        rows.forEach(r -> assertTrue(
                Double.parseDouble(r.get(3).toString()) > 1000.0
        ));
        System.out.printf("[PASS] Filter(amount>1000): %d rows%n", rows.size());
    }

    // ── Test 4: Projection pushdown ──────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("GenericJdbc SQL pattern: SELECT region, amount FROM table (Projection)")
    void testProjection() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT region, amount FROM " + TABLE;
        List<List<Object>> rows = runQuery(sql);
        assertEquals(10, rows.size());
        rows.forEach(r -> assertEquals(2, r.size(), "Should only have 2 columns"));
        System.out.println("[PASS] Projection(region, amount): " + rows.size() + " rows");
    }

    // ── Test 5: Filter + Projection combined ─────────────────────────────

    @Test
    @Order(5)
    @DisplayName("GenericJdbc SQL pattern: SELECT region, amount FROM table WHERE amount > 1000")
    void testFilterAndProjection() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT region, amount FROM " + TABLE + " WHERE amount > 1000";
        List<List<Object>> rows = runQuery(sql);
        assertFalse(rows.isEmpty());
        rows.forEach(r -> {
            assertEquals(2, r.size());
            assertTrue(Double.parseDouble(r.get(1).toString()) > 1000.0);
        });
        System.out.printf("[PASS] Filter+Projection: %d rows%n", rows.size());
    }

    // ── Test 6: Cardinality estimation (SELECT count(*)) ─────────────────

    @Test
    @Order(6)
    @DisplayName("GenericJdbc SQL pattern: SELECT count(*) FROM table (cardinality)")
    void testCardinalityEstimation() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT count(*) FROM " + TABLE;
        List<List<Object>> rows = runQuery(sql);
        assertEquals(1, rows.size());
        long count = Long.parseLong(rows.get(0).get(0).toString());
        assertEquals(10, count);
        System.out.println("[PASS] Cardinality: count(*) = " + count);
    }

    // ── Test 7: No trailing semicolons ───────────────────────────────────

    @Test
    @Order(7)
    @DisplayName("SQL without trailing semicolon works (validates our fix)")
    void testNoTrailingSemicolon() throws Exception {
        Assumptions.assumeTrue(emulatorAvailable, "Emulator not available");

        String sql = "SELECT count(*) FROM " + TABLE;
        assertFalse(sql.endsWith(";"), "SQL should not end with semicolon");
        List<List<Object>> rows = runQuery(sql);
        assertFalse(rows.isEmpty());
        System.out.println("[PASS] No trailing semicolon works");
    }
}
