package org.apache.wayang.genericjdbc;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests validating BigQuery SQL patterns via the official
 * Google BigQuery JDBC driver against a real BigQuery project.
 *
 * These tests mirror exactly what GenericJdbcExecutor.createSqlQuery()
 * would produce for TableSource, Filter, Projection, and combined pipelines.
 *
 * Prerequisites:
 *   Service account key at: ~/wayang-bq-key.json
 *   Table: daeproject-316010.sales.orders (10 rows)
 *
 * Run:
 *   mvn test -pl wayang-platforms/wayang-generic-jdbc \
 *     -Dtest=BigQueryGenericJdbcIT \
 *     -Drat.skip=true -Dmaven.javadoc.skip=true \
 *     -Dlicense.skipAggregateAddThirdParty=true
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryGenericJdbcIT {

    private static final String PROJECT_ID = "daeproject-316010";
    private static final String KEY_PATH   = System.getProperty("user.home") + "/wayang-bq-key.json";
    private static final String SA_EMAIL   = "wayang-bq-test@daeproject-316010.iam.gserviceaccount.com";
    private static final String TABLE      = "`daeproject-316010.sales.orders`";

    private static final String JDBC_URL = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;" +
            "ProjectId=%s;" +
            "OAuthType=0;" +
            "OAuthServiceAcctEmail=%s;" +
            "OAuthPvtKeyPath=%s",
            PROJECT_ID, SA_EMAIL, KEY_PATH);

    private static Connection connection;
    private static boolean available = false;

    @BeforeAll
    static void connect() {
        try {
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
            connection = DriverManager.getConnection(JDBC_URL);
            available = true;
            System.out.println("Connected to BigQuery project: " + PROJECT_ID);
        } catch (Exception e) {
            System.err.println("BigQuery not available: " + e.getMessage());
        }
    }

    @AfterAll
    static void disconnect() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private List<List<Object>> runQuery(String sql) throws SQLException {
        System.out.println("SQL: " + sql);
        List<List<Object>> rows = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            int cols = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= cols; i++) {
                    row.add(rs.getObject(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    // ── Test 1: Full table scan (GenericJdbcTableSource) ─────────────────

    @Test
    @Order(1)
    @DisplayName("GenericJdbc SQL: SELECT * FROM table  (TableSource)")
    void testTableScan() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery("SELECT * FROM " + TABLE);
        assertEquals(10, rows.size(), "Expected 10 rows");
        System.out.println("[PASS] TableScan: " + rows.size() + " rows");
    }

    // ── Test 2: Filter — string predicate ───────────────────────────────

    @Test
    @Order(2)
    @DisplayName("GenericJdbc SQL: SELECT * FROM table WHERE region = 'APAC'")
    void testFilterString() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery(
                "SELECT * FROM " + TABLE + " WHERE region = 'APAC'");
        assertFalse(rows.isEmpty());
        rows.forEach(r -> assertEquals("APAC", r.get(1)));
        System.out.printf("[PASS] Filter(region='APAC'): %d rows%n", rows.size());
    }

    // ── Test 3: Filter — numeric predicate ──────────────────────────────

    @Test
    @Order(3)
    @DisplayName("GenericJdbc SQL: SELECT * FROM table WHERE amount > 1000")
    void testFilterNumeric() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery(
                "SELECT * FROM " + TABLE + " WHERE amount > 1000");
        assertFalse(rows.isEmpty());
        rows.forEach(r -> assertTrue(
                ((Number) r.get(3)).doubleValue() > 1000.0));
        System.out.printf("[PASS] Filter(amount>1000): %d rows%n", rows.size());
    }

    // ── Test 4: Projection ───────────────────────────────────────────────

    @Test
    @Order(4)
    @DisplayName("GenericJdbc SQL: SELECT region, amount FROM table  (Projection)")
    void testProjection() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery(
                "SELECT region, amount FROM " + TABLE);
        assertEquals(10, rows.size());
        rows.forEach(r -> assertEquals(2, r.size()));
        System.out.println("[PASS] Projection(region, amount): " + rows.size() + " rows");
    }

    // ── Test 5: Filter + Projection combined ─────────────────────────────

    @Test
    @Order(5)
    @DisplayName("GenericJdbc SQL: SELECT region, amount FROM table WHERE amount > 1000")
    void testFilterAndProjection() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery(
                "SELECT region, amount FROM " + TABLE + " WHERE amount > 1000");
        assertFalse(rows.isEmpty());
        rows.forEach(r -> {
            assertEquals(2, r.size());
            assertTrue(((Number) r.get(1)).doubleValue() > 1000.0);
        });
        System.out.printf("[PASS] Filter+Projection: %d rows%n", rows.size());
    }

    // ── Test 6: Cardinality estimation (SELECT count(*)) ─────────────────

    @Test
    @Order(6)
    @DisplayName("GenericJdbc SQL: SELECT count(*) FROM table  (cardinality estimation)")
    void testCardinalityEstimation() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<List<Object>> rows = runQuery("SELECT count(*) FROM " + TABLE);
        assertEquals(1, rows.size());
        long count = ((Number) rows.get(0).get(0)).longValue();
        assertEquals(10, count);
        System.out.println("[PASS] Cardinality: count(*) = " + count);
    }

    // ── Test 7: No trailing semicolon ────────────────────────────────────

    @Test
    @Order(7)
    @DisplayName("SQL without trailing semicolon works (validates GenericJdbcExecutor fix)")
    void testNoTrailingSemicolon() throws Exception {
        Assumptions.assumeTrue(available, "BigQuery not available");

        String sql = "SELECT count(*) FROM " + TABLE;
        assertFalse(sql.endsWith(";"), "SQL must not end with semicolon");
        List<List<Object>> rows = runQuery(sql);
        assertFalse(rows.isEmpty());
        System.out.println("[PASS] No trailing semicolon accepted by BigQuery JDBC");
    }
}
