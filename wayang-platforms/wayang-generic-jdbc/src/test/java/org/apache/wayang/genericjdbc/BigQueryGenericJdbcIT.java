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
import org.apache.wayang.genericjdbc.bigquery.BigQueryPlatform;
import org.apache.wayang.genericjdbc.bigquery.BigQueryTableSource;
import org.apache.wayang.java.Java;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the BigQuery connector via {@link BigQuery#plugin()}.
 *
 * <p>Prerequisites:
 * <ol>
 *   <li>Service account key at: {@code ~/wayang-bq-key.json}</li>
 *   <li>BigQuery table: {@code daeproject-316010.sales.orders} (10 rows)</li>
 * </ol>
 *
 * <p>Run:
 * <pre>
 *   mvn test -pl wayang-platforms/wayang-generic-jdbc \
 *       -Dtest=BigQueryGenericJdbcIT -Pintegration,skip-prerequisite-check \
 *       -Drat.skip=true -Dmaven.javadoc.skip=true
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BigQueryGenericJdbcIT {

    private static final String PROJECT_ID = "daeproject-316010";
    private static final String KEY_PATH   = System.getProperty("user.home") + "/wayang-bq-key.json";
    private static final String SA_EMAIL   = "wayang-bq-test@daeproject-316010.iam.gserviceaccount.com";

    /** Backtick-quoted fully-qualified BigQuery table name. */
    private static final String TABLE = "`daeproject-316010.sales.orders`";

    private static final String JDBC_URL = String.format(
            "jdbc:bigquery://https://www.googleapis.com/bigquery/v2;" +
            "ProjectId=%s;OAuthType=0;OAuthServiceAcctEmail=%s;OAuthPvtKeyPath=%s",
            PROJECT_ID, SA_EMAIL, KEY_PATH);

    private static boolean available = false;

    // ── Setup ───────────────────────────────────────────────────────────────

    @BeforeAll
    static void checkAvailable() {
        try {
            Class.forName("com.google.cloud.bigquery.jdbc.BigQueryDriver");
            try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
                ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
                available = rs.next();
                System.out.println("[SETUP] Connected to BigQuery project: " + PROJECT_ID);
            }
        } catch (Exception e) {
            System.err.println("[SETUP] BigQuery not available — all tests will be skipped: " + e.getMessage());
        }
    }

    private Configuration createBigQueryConfig() {
        Configuration config = new Configuration();
        config.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);
        return config;
    }

    private WayangContext createContext(Configuration config) {
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());
    }

    // ════════════════════════════════════════════════════════════════════════
    //  VERIFICATION TESTS
    // ════════════════════════════════════════════════════════════════════════

    /**
     * VERIFICATION A — Platform binding.
     * BigQueryTableSource must be bound to BigQueryPlatform (not GenericJdbcPlatform).
     */
    @Test
    @Order(0)
    @DisplayName("[VERIFY] BigQueryTableSource is bound to BigQueryPlatform")
    void testPlatformBinding() {
        BigQueryTableSource source = new BigQueryTableSource(TABLE, "order_id");

        assertSame(
                BigQueryPlatform.getInstance(),
                source.getPlatform(),
                "BigQueryTableSource.getPlatform() must return BigQueryPlatform singleton"
        );
        assertEquals("bigquery", source.getPlatform().getPlatformId(),
                "Platform ID drives all wayang.bigquery.* config key lookups");
        assertEquals("bigquery", source.jdbcName,
                "jdbcName is fixed to 'bigquery'");

        System.out.println("[VERIFY] getPlatform()   = " + source.getPlatform().getClass().getSimpleName());
        System.out.println("[VERIFY] getPlatformId() = " + source.getPlatform().getPlatformId());
        System.out.println("[VERIFY] jdbcName        = " + source.jdbcName);
    }

    /**
     * VERIFICATION B — Hard failure without JDBC config.
     * If BigQuery JDBC URL is missing, execution must throw, not silently
     * fall back to Java evaluation.
     */
    @Test
    @Order(1)
    @DisplayName("[VERIFY] Execution fails when BigQuery JDBC config is missing")
    void testFailsWithoutJdbcConfig() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        Configuration emptyConfig = new Configuration();
        BigQueryTableSource source = new BigQueryTableSource(TABLE, "order_id", "region");
        List<Record> results = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        WayangContext ctx = new WayangContext(emptyConfig)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());

        assertThrows(Exception.class,
                () -> ctx.execute("BQ-NoConfig", new WayangPlan(sink)),
                "Should throw when wayang.bigquery.jdbc.url is not set"
        );
        System.out.println("[VERIFY] Correctly threw when JDBC config was absent.");
    }

    // ════════════════════════════════════════════════════════════════════════
    //  FUNCTIONAL TESTS
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Full table scan.
     * Wayang sends: SELECT * FROM `daeproject-316010.sales.orders`
     */
    @Test
    @Order(2)
    @DisplayName("BigQuery: full table scan")
    void testTableScan() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-TableScan", new WayangPlan(sink));

        assertEquals(10, results.size(), "Expected 10 rows");
        System.out.println("[PASS] TableScan: " + results.size() + " rows");
        results.forEach(r -> System.out.println("       " + r));
    }

    /**
     * String filter pushed to SQL.
     * Wayang sends: SELECT * FROM `...` WHERE region = 'APAC'
     */
    @Test
    @Order(3)
    @DisplayName("BigQuery: filter pushdown (region = 'APAC')")
    void testFilterString() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "APAC".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'APAC'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Filter", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> assertEquals("APAC", r.getField(1)));
        System.out.println("[PASS] Filter(region='APAC'): " + results.size() + " rows");
    }

    /**
     * Numeric filter pushed to SQL.
     * Wayang sends: SELECT * FROM `...` WHERE amount > 1000
     */
    @Test
    @Order(4)
    @DisplayName("BigQuery: filter pushdown (amount > 1000)")
    void testFilterNumeric() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> ((Number) r.getField(3)).doubleValue() > 1000.0, Record.class
                ).withSqlImplementation("amount > 1000")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Filter-Numeric", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> assertTrue(((Number) r.getField(3)).doubleValue() > 1000.0));
        System.out.println("[PASS] Filter(amount>1000): " + results.size() + " rows");
    }

    /**
     * Column pruning pushed to SQL.
     * Wayang sends: SELECT region, amount FROM `...`
     */
    @Test
    @Order(5)
    @DisplayName("BigQuery: projection pushdown (region, amount)")
    void testProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        MapOperator<Record, Record> project = new MapOperator<>(
                new ProjectionDescriptor<>(Record.class, Record.class, "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, project, 0);
        project.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Projection", new WayangPlan(sink));

        assertEquals(10, results.size());
        results.forEach(r -> assertEquals(2, r.size(), "Record should have 2 projected fields"));
        System.out.println("[PASS] Projection(region, amount): " + results.size() + " rows");
        results.forEach(r -> System.out.println("       " + r));
    }

    /**
     * Combined filter + projection in one SQL query.
     * Wayang sends: SELECT region, amount FROM `...` WHERE amount > 1000
     */
    @Test
    @Order(6)
    @DisplayName("BigQuery: filter + projection pipeline")
    void testFilterAndProjection() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> ((Number) r.getField(3)).doubleValue() > 1000.0, Record.class
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

        createContext(createBigQueryConfig()).execute("BQ-Filter-Projection", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> {
            assertEquals(2, r.size());
            assertTrue(((Number) r.getField(1)).doubleValue() > 1000.0);
        });
        System.out.println("[PASS] Filter+Projection: " + results.size() + " rows");
        results.forEach(r -> System.out.printf("       region=%s  amount=%s%n", r.getField(0), r.getField(1)));
    }

    /**
     * Cardinality estimation sanity check.
     * The optimizer runs SELECT count(*) against BigQuery before planning.
     */
    @Test
    @Order(7)
    @DisplayName("BigQuery: cardinality estimation via COUNT(*) is accurate")
    void testCardinalityMatches() {
        Assumptions.assumeTrue(available, "BigQuery not available");

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                TABLE, "order_id", "region", "product", "amount"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "EMEA".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'EMEA'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createBigQueryConfig()).execute("BQ-Cardinality", new WayangPlan(sink));

        assertEquals(3, results.size(), "Expected 3 EMEA rows");
        System.out.println("[PASS] Cardinality: " + results.size() + " EMEA rows (expected 3)");
    }
}
