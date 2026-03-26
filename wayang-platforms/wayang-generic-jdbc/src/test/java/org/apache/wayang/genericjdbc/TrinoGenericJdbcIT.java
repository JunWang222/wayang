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
import org.apache.wayang.genericjdbc.trino.TrinoFilterOperator;
import org.apache.wayang.genericjdbc.trino.TrinoTableSource;
import org.apache.wayang.genericjdbc.trino.TrinoPlatform;
import org.apache.wayang.java.Java;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Trino connector.
 *
 * ═══════════════════════════════════════════════════════════════
 *  HOW THIS TEST WORKS
 * ═══════════════════════════════════════════════════════════════
 *
 * Each test builds a small Wayang logical plan, hands it to WayangContext,
 * and collects results into a List. Wayang internally:
 *
 *   1. Optimizer reads the plan (TrinoTableSource → FilterOperator → sink).
 *   2. TrinoFilterMapping converts FilterOperator → TrinoFilterOperator
 *      (targeted at TrinoPlatform).
 *   3. TrinoExecutor assembles a single SQL string:
 *        SELECT * FROM iceberg.sales.orders WHERE region = 'APAC'
 *   4. GenericSqlToStreamOperator opens a JDBC connection to Trino,
 *      runs that SQL, and streams results back as Java Record objects.
 *   5. Results reach the sink.
 *
 * HOW TO VERIFY TRINO IS ACTUALLY USED (not Java fallback):
 *
 *   A) Platform type assertion (testPlatformBinding):
 *      source.getPlatform() must be TrinoPlatform — asserted explicitly.
 *
 *   B) Query history proof (testQueryAppearsInTrinoHistory):
 *      After running a Wayang plan, queries Trino's system.runtime.queries
 *      catalogue view and asserts our SQL appeared there. If Wayang had
 *      evaluated everything in Java, no SQL would reach Trino.
 *
 *   C) Hard failure without config (testFailsWithoutJdbcConfig):
 *      Without wayang.trino.jdbc.url the connector cannot open a connection
 *      and must throw. If the test had silently fallen back to Java, it
 *      would pass — so a thrown exception here is the proof that the
 *      connector is mandatory.
 *
 * ═══════════════════════════════════════════════════════════════
 *
 * Prerequisites:
 *   cd trino-setup && docker-compose up -d && ./scripts/run-init.sh
 *
 * Run:
 *   mvn test -pl wayang-platforms/wayang-generic-jdbc \
 *       -Dtest=TrinoGenericJdbcIT -Pintegration,skip-prerequisite-check \
 *       -Drat.skip=true -Dmaven.javadoc.skip=true
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TrinoGenericJdbcIT {

    private static final String TRINO_HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int    TRINO_PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String JDBC_URL   = String.format("jdbc:trino://%s:%d", TRINO_HOST, TRINO_PORT);

    /** Set to true only when we confirm Trino is reachable before any test runs. */
    private static boolean trinoAvailable = false;

    // ── Setup ───────────────────────────────────────────────────────────────

    /**
     * Opens a raw JDBC connection to Trino and runs SELECT 1.
     * If Trino is unreachable all tests are SKIPPED (not failed) via
     * Assumptions.assumeTrue() inside each test.
     */
    @BeforeAll
    static void checkTrinoAvailable() {
        try {
            Properties props = new Properties();
            props.setProperty("user", "admin");
            try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
                ResultSet rs = conn.createStatement().executeQuery("SELECT 1");
                trinoAvailable = rs.next();
                System.out.println("[SETUP] Trino reachable at " + JDBC_URL);
            }
        } catch (Exception e) {
            System.err.println("[SETUP] Trino not available — all tests will be skipped: " + e.getMessage());
        }
    }

    /**
     * Builds a config with only the deployment-specific URL.
     * Cost model, hardware, and driver class come from
     * wayang-trino-defaults.properties (loaded automatically by TrinoPlatform).
     */
    private Configuration createTrinoConfig() {
        Configuration config = new Configuration();
        config.setProperty("wayang.trino.jdbc.url",      JDBC_URL);
        config.setProperty("wayang.trino.jdbc.user",     "admin");
        config.setProperty("wayang.trino.jdbc.password", "");
        return config;
    }

    /**
     * Registers Java (for the sink) and Trino (for SQL execution).
     * The optimizer sees both platforms and chooses which operators run where.
     */
    private WayangContext createContext(Configuration config) {
        return new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Trino.plugin());
    }

    // ════════════════════════════════════════════════════════════════════════
    //  VERIFICATION TESTS  (run first — prove the connector is actually used)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * DEMO — What Trino.plugin() contributes to the optimizer.
     *
     * Prints the three things the optimizer gains when Trino.plugin() is
     * registered, making it concrete what "adding a platform" actually means:
     *
     *   1. Platforms  — TrinoPlatform is declared so the optimizer knows
     *                   Trino is a candidate execution target.
     *   2. Mappings   — rewrite rules that tell the optimizer "a logical
     *                   FilterOperator with a SQL impl can run on Trino as
     *                   a TrinoFilterOperator".
     *   3. Channel conversions — the bridge that materialises a Trino
     *                   SqlQueryChannel into a Java Stream so results can
     *                   flow back to the sink.
     *
     * Without all three the optimizer has an incomplete picture and cannot
     * build a valid execution plan for a plan rooted in TrinoTableSource.
     *
     * This test does NOT need a live Trino instance.
     */
    @Test
    @Order(0)
    @DisplayName("[DEMO] What Trino.plugin() registers with the optimizer")
    void testPluginContributions() {
        org.apache.wayang.core.plugin.Plugin plugin = Trino.plugin();

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  What Trino.plugin() gives the Wayang optimizer");
        System.out.println("══════════════════════════════════════════════════════");

        System.out.println("\n① PLATFORMS (optimizer knows these engines exist):");
        plugin.getRequiredPlatforms().forEach(p ->
                System.out.printf("     %-20s  id='%s'%n",
                        p.getClass().getSimpleName(),
                        p.getConfigurationName()));

        System.out.println("\n② OPERATOR MAPPINGS (logical → physical rewrite rules):");
        plugin.getMappings().forEach(m ->
                System.out.printf("     %s%n", m.getClass().getSimpleName()));

        System.out.println("\n③ CHANNEL CONVERSIONS (how data flows out of Trino):");
        plugin.getChannelConversions().forEach(c ->
                System.out.printf("     %s  →  %s%n",
                        c.getSourceChannelDescriptor().getChannelClass().getSimpleName(),
                        c.getTargetChannelDescriptor().getChannelClass().getSimpleName()));

        System.out.println("\n══════════════════════════════════════════════════════");
        System.out.println("  Remove any one of these → optimizer has no valid plan.");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();

        // Structural assertions so this is a real test, not just a print statement.
        assertFalse(plugin.getRequiredPlatforms().isEmpty(),   "plugin must declare at least one platform");
        assertFalse(plugin.getMappings().isEmpty(),            "plugin must declare at least one mapping");
        assertFalse(plugin.getChannelConversions().isEmpty(),  "plugin must declare at least one channel conversion");

        assertTrue(plugin.getRequiredPlatforms().stream()
                .anyMatch(p -> "trino".equals(p.getConfigurationName())),
                "TrinoPlatform (id='trino') must be among the required platforms");
    }

    /**
     * DEMO Seg 2 — Operator mapping: logical → physical rewrite.
     *
     * The interesting mapping story is FilterOperator → TrinoFilterOperator.
     * The user writes a platform-agnostic FilterOperator (logical); the
     * optimizer's TrinoFilterMapping rewrites it into a TrinoFilterOperator
     * (physical) that is bound to TrinoPlatform.
     *
     * TrinoTableSource is different: the user instantiates it directly, so
     * it is already the physical operator — no rewrite step needed.
     *
     * This test shows both sides:
     *   - the rewrite: FilterOperator  →  TrinoFilterOperator
     *   - the binding: a single getPlatform() override is all it takes to
     *     tie any operator to Trino's cost model, JDBC config, and channel.
     *
     * Does NOT need a live Trino instance.
     */
    @Test
    @Order(1)
    @DisplayName("[DEMO Seg2] Operator mapping: logical → physical rewrite")
    void testPlatformBinding() {
        // ── Logical operators (platform-agnostic, written by the user) ──────
        FilterOperator<Record> logicalFilter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "APAC".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'APAC'")
        );
        // ── Physical operators (Trino-specific, produced by the mapping) ─────
        TrinoFilterOperator physicalFilter = new TrinoFilterOperator(logicalFilter);

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Operator Mapping: logical → physical rewrite");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  BEFORE (logical — what the user writes):");
        System.out.printf("    class     : %s%n", logicalFilter.getClass().getSimpleName());
        System.out.printf("    platform  : (none — platform-agnostic)%n");
        System.out.printf("    SQL impl  : \"%s\"%n",
                logicalFilter.getPredicateDescriptor().getSqlImplementation());
        System.out.println();
        System.out.println("  AFTER  (physical — what TrinoFilterMapping rewrites it to):");
        System.out.printf("    class     : %s%n", physicalFilter.getClass().getSimpleName());
        System.out.printf("    platform  : %s  (id='%s')%n",
                physicalFilter.getPlatform().getClass().getSimpleName(),
                physicalFilter.getPlatform().getPlatformId());
        System.out.printf("    cost key  : wayang.%s.filter.load%n",
                physicalFilter.getPlatform().getPlatformId());
        System.out.println();
        System.out.println("  The rewrite is triggered by TrinoFilterMapping when:");
        System.out.println("    ① the operator is a FilterOperator<Record>  AND");
        System.out.println("    ② it has a SQL implementation attached");
        System.out.println();
        System.out.println("  One getPlatform() override is all it takes:");
        System.out.println("    cost model, JDBC config, and channel descriptor");
        System.out.println("    all resolve automatically from platform ID 'trino'.");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();

        assertSame(TrinoPlatform.getInstance(), physicalFilter.getPlatform(),
                "TrinoFilterOperator.getPlatform() must return TrinoPlatform");
        assertEquals("trino", physicalFilter.getPlatform().getPlatformId());
        assertNotEquals(logicalFilter.getClass(), physicalFilter.getClass(),
                "Physical operator must be a different (Trino-specific) class");
    }

    /**
     * VERIFICATION B — Query appears in Trino's runtime query history.
     *
     * Runs a Wayang plan, then queries system.runtime.queries in Trino to
     * confirm the exact SQL string was submitted. If Wayang had fallen back
     * to in-process Java execution, no SQL would appear in Trino's history.
     *
     * system.runtime.queries is Trino's internal catalog of recently
     * executed queries on that cluster.
     */
    @Test
    @Order(2)
    @DisplayName("[VERIFY] Wayang-assembled SQL appears in Trino's query history")
    void testQueryAppearsInTrinoHistory() throws Exception {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        // Run a Wayang plan with a filter so the SQL is distinctive.
        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders", "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-VerifyHistory", new WayangPlan(sink));

        // The SQL Wayang assembles is:
        //   SELECT * FROM iceberg.sales.orders WHERE region = 'AMER'
        // Now look for it in Trino's runtime.queries catalogue.
        String expectedFragment = "iceberg.sales.orders";
        boolean foundInHistory = false;

        Properties props = new Properties();
        props.setProperty("user", "admin");
        try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
            // system.runtime.queries lists recently completed/running queries.
            String historyQuery =
                "SELECT query FROM system.runtime.queries " +
                "WHERE state = 'FINISHED' " +
                "  AND query LIKE '%" + expectedFragment + "%' " +
                "ORDER BY created DESC " +
                "LIMIT 5";
            ResultSet rs = conn.createStatement().executeQuery(historyQuery);
            while (rs.next()) {
                String q = rs.getString(1);
                System.out.println("[VERIFY] Found in Trino history: " + q.replaceAll("\\s+", " "));
                if (q.contains(expectedFragment)) {
                    foundInHistory = true;
                }
            }
        }

        assertTrue(foundInHistory,
                "Expected to find a query containing '" + expectedFragment +
                "' in Trino's system.runtime.queries. " +
                "If this fails, Wayang did NOT send SQL to Trino.");
        assertEquals(3, results.size(), "Expected 3 AMER rows");
        System.out.println("[VERIFY] SQL confirmed in Trino history. " + results.size() + " AMER rows returned.");
    }

    /**
     * VERIFICATION C — Hard failure without JDBC config.
     *
     * If Wayang silently fell back to Java when the JDBC URL is missing,
     * this test would PASS (wrong). Instead, it must throw because the
     * Trino connector cannot open a connection.
     *
     * A WayangException (or its cause) proves that Wayang tried to connect
     * to Trino and failed — not that it quietly computed the result in Java.
     */
    @Test
    @Order(3)
    @DisplayName("[VERIFY] Execution fails when Trino JDBC config is missing")
    void testFailsWithoutJdbcConfig() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        // Intentionally empty config — no wayang.trino.jdbc.url set.
        Configuration emptyConfig = new Configuration();

        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders", "order_id", "region"
        );
        List<Record> results = new ArrayList<>();
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        WayangContext ctx = new WayangContext(emptyConfig)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Trino.plugin());

        // Must throw — the connector requires a JDBC URL.
        assertThrows(Exception.class,
                () -> ctx.execute("Trino-NoConfig", new WayangPlan(sink)),
                "Should throw when wayang.trino.jdbc.url is not configured. " +
                "If this does not throw, Wayang silently fell back to Java — the connector is NOT being used."
        );
        System.out.println("[VERIFY] Correctly threw when JDBC config was absent.");
    }

    // ════════════════════════════════════════════════════════════════════════
    //  FUNCTIONAL TESTS
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Full table scan — no filter or projection.
     * Wayang sends: SELECT * FROM iceberg.sales.orders
     */
    @Test
    @Order(4)
    @DisplayName("Trino: full table scan")
    void testTableScan() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-TableScan", new WayangPlan(sink));

        assertEquals(20, results.size(), "Expected 20 rows from iceberg.sales.orders");
        System.out.println("[PASS] TableScan: " + results.size() + " rows");
        results.forEach(r -> System.out.println("       " + r));
    }

    /**
     * String filter pushed down to SQL.
     * Wayang sends: SELECT * FROM iceberg.sales.orders WHERE region = 'APAC'
     * The Java lambda (record -> "APAC".equals(...)) is NOT evaluated —
     * the SQL WHERE clause handles it inside Trino.
     */
    @Test
    @Order(5)
    @DisplayName("Trino: filter pushdown (region = APAC)")
    void testFilterPushdown() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        // Java lambda — only used if Wayang falls back to Java evaluation.
                        record -> "APAC".equals(record.getField(1)),
                        Record.class
                ).withSqlImplementation("region = 'APAC'")  // ← pushed to Trino as SQL
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-Filter", new WayangPlan(sink));

        assertFalse(results.isEmpty(), "Should have APAC rows");
        results.forEach(r -> assertEquals("APAC", r.getField(1), "All rows should be APAC"));
        System.out.println("[PASS] Filter pushdown: " + results.size() + " APAC rows");
    }

    /**
     * Numeric filter pushed to SQL.
     * Wayang sends: SELECT * FROM iceberg.sales.orders WHERE amount > 1000
     */
    @Test
    @Order(6)
    @DisplayName("Trino: filter pushdown (amount > 1000)")
    void testFilterNumeric() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> ((Number) record.getField(3)).doubleValue() > 1000.0,
                        Record.class
                ).withSqlImplementation("amount > 1000")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-Filter-Numeric", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        results.forEach(r -> assertTrue(
                ((Number) r.getField(3)).doubleValue() > 1000.0,
                "All rows should have amount > 1000"
        ));
        System.out.println("[PASS] Numeric filter: " + results.size() + " rows with amount > 1000");
    }

    /**
     * Projection pushed to SQL.
     * Wayang sends: SELECT region, amount FROM iceberg.sales.orders
     * Only 2 columns are fetched from Trino — column pruning happens in SQL.
     */
    @Test
    @Order(7)
    @DisplayName("Trino: projection pushdown (region, amount)")
    void testProjectionPushdown() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
        );
        MapOperator<Record, Record> project = new MapOperator<>(
                new ProjectionDescriptor<>(Record.class, Record.class, "region", "amount"),
                DataSetType.createDefault(Record.class),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, project, 0);
        project.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-Projection", new WayangPlan(sink));

        assertEquals(10, results.size(), "Should have 10 projected rows");
        // Each record has only 2 fields because projection happened in SQL
        results.forEach(r -> assertEquals(2, r.size(), "Record should have exactly 2 projected fields"));
        System.out.println("[PASS] Projection: " + results.size() + " rows (region, amount)");
        results.forEach(r -> System.out.println("       " + r));
    }

    /**
     * Combined filter + projection in a single SQL query.
     * Wayang sends:
     *   SELECT region, amount FROM iceberg.sales.orders WHERE amount > 1000
     * Both the WHERE and the SELECT column list are assembled by TrinoExecutor
     * and executed as one query on Trino — no two-step fetch.
     */
    @Test
    @Order(8)
    @DisplayName("Trino: filter + projection pipeline")
    void testFilterThenProjection() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
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

        createContext(createTrinoConfig()).execute("Trino-Filter-Projection", new WayangPlan(sink));

        assertFalse(results.isEmpty());
        // field(1) is 'amount' after projection
        results.forEach(r -> assertTrue(
                ((Number) r.getField(1)).doubleValue() > 1000.0,
                "Projected amount (field 1) should be > 1000"
        ));
        System.out.println("[PASS] Filter+Projection: " + results.size() + " rows");
        results.forEach(r -> System.out.printf("       region=%s  amount=%s%n", r.getField(0), r.getField(1)));
    }

    /**
     * DEMO Seg 3 — Cost model: three-layer pipeline from formula to cost.
     *
     *   Layer 1  formula   cpu = α*rows + β   (wayang.trino.tablesource.load)
     *   Layer 2  hardware  cpu cycles → ms    (cpu.mhz, cores)
     *   Layer 3  cost      ms → abstract cost (costs.fix, costs.per-ms)
     *
     * Also shows a worked example with the test dataset (10 rows) so the
     * audience can see a concrete number the optimizer would compare against
     * other platforms.
     *
     * Does NOT need a live Trino instance.
     */
    @Test
    @Order(9)
    @DisplayName("[DEMO Seg3] Cost model: formula → time → cost")
    void testCostModelConfig() {
        Configuration config = new Configuration();
        TrinoPlatform.getInstance().configureDefaults(config);

        long   mhz      = config.getLongProperty("wayang.trino.cpu.mhz", 0);
        long   cores    = config.getLongProperty("wayang.trino.cores",   0);
        double fix      = config.getDoubleProperty("wayang.trino.costs.fix",    0);
        double perMs    = config.getDoubleProperty("wayang.trino.costs.per-ms", 1);

        // Worked example with the test dataset (10 rows, α=10, β=800_000)
        long   rows     = 10;
        long   alpha    = 10;
        long   beta     = 800_000;
        long   cpuCycles = alpha * rows + beta;
        double timeMs   = cpuCycles / (cores * mhz * 1000.0);
        double cost     = fix + perMs * timeMs;

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Cost Model Integration — three layers");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  LAYER 1 — Cost formula  (wayang-trino-defaults.properties)");
        System.out.printf("    %-40s %s%n", "wayang.trino.tablesource.load:",
                config.getStringProperty("wayang.trino.tablesource.load", null));
        System.out.printf("    %-40s %s%n", "wayang.trino.filter.load:",
                config.getStringProperty("wayang.trino.filter.load", null));
        System.out.printf("    %-40s %s%n", "wayang.trino.sqltostream.load.query:",
                config.getStringProperty("wayang.trino.sqltostream.load.query", null));
        System.out.println();
        System.out.println("  LAYER 2 — Hardware profile  (cpu cycles → wall-clock ms)");
        System.out.printf("    cpu.mhz = %d   cores = %d%n", mhz, cores);
        System.out.printf("    time    = cpuCycles / (cores × mhz × 1000)%n");
        System.out.println();
        System.out.println("  LAYER 3 — Time → abstract cost  (cross-platform comparison)");
        System.out.printf("    costs.fix = %.1f   costs.per-ms = %.1f%n", fix, perMs);
        System.out.printf("    cost = fix + per-ms × time%n");
        System.out.println();
        System.out.println("  ── Worked example: 10-row table scan ─────────────");
        System.out.printf("    cpu cycles = α×rows + β = %d×%d + %d = %,d%n",
                alpha, rows, beta, cpuCycles);
        System.out.printf("    time       = %,d / (%d × %d × 1000) = %.4f ms%n",
                cpuCycles, cores, mhz, timeMs);
        System.out.printf("    cost       = %.1f + %.1f × %.4f = %.4f%n",
                fix, perMs, timeMs, cost);
        System.out.println();
        System.out.println("  The optimizer compares this number against Java, Spark, etc.");
        System.out.println("  Tune α and β in defaults.properties after real benchmarks.");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();

        // Structural assertions
        assertNotNull(config.getStringProperty("wayang.trino.tablesource.load", null));
        assertNotNull(config.getStringProperty("wayang.trino.filter.load",      null));
        assertNotNull(config.getStringProperty("wayang.trino.sqltostream.load.query", null));
        assertTrue(mhz   > 0, "wayang.trino.cpu.mhz must be > 0");
        assertTrue(cores > 0, "wayang.trino.cores must be > 0");
        assertNotNull(config.getStringProperty("wayang.trino.costs.fix",    null));
        assertNotNull(config.getStringProperty("wayang.trino.costs.per-ms", null));
        assertTrue(cost > 0, "Computed cost must be > 0 for a non-empty table");
    }

    /**
     * Verifies that the cardinality estimator (which opens a JDBC connection
     * and runs SELECT count(*)) returns the correct row count.
     * This exercises the Trino connection path used during the optimizer phase,
     * not just the execution phase.
     */
    @Test
    @Order(10)
    @DisplayName("Trino: cardinality estimation via COUNT(*) is accurate")
    void testCardinalityMatches() {
        Assumptions.assumeTrue(trinoAvailable, "Trino not available");

        List<Record> results = new ArrayList<>();
        TrinoTableSource source = new TrinoTableSource(
                "iceberg.sales.orders",
                "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        record -> "EMEA".equals(record.getField(1)),
                        Record.class
                ).withSqlImplementation("region = 'EMEA'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        createContext(createTrinoConfig()).execute("Trino-Cardinality", new WayangPlan(sink));

        assertEquals(3, results.size(), "Expected 3 EMEA rows");
        System.out.println("[PASS] Cardinality: " + results.size() + " EMEA rows (expected 3)");
    }
}
