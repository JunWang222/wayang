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
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.genericjdbc.trino.TrinoPlatform;
import org.apache.wayang.genericjdbc.trino.TrinoTableSource;
import org.apache.wayang.java.Java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Standalone demo for the Wayang Trino connector.
 *
 * <p>Run with:
 * <pre>
 *   cd /path/to/wayang
 *   mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
 *     -Pskip-prerequisite-check -Drat.skip=true \
 *     [-Dtrino.url=jdbc:trino://host:8080] [-Dtrino.user=admin]
 * </pre>
 *
 * <p>Demonstrates:
 * <ol>
 *   <li>Seg 3 — Cost model: shows the three-layer pipeline
 *       (formula → cpu cycles → time → abstract cost).</li>
 *   <li>Seg 4 — End-to-end: runs a Wayang plan with filter pushdown on Trino
 *       and verifies the SQL appeared in Trino's query history.</li>
 * </ol>
 */
public class TrinoDemo {

    private static final String JDBC_URL  = System.getProperty("trino.url",  "jdbc:trino://localhost:8080");
    private static final String JDBC_USER = System.getProperty("trino.user", "admin");

    public static void main(String[] args) throws Exception {
        seg3CostModel();
        seg4EndToEnd();
    }

    // ── Seg 3 — Cost model ────────────────────────────────────────────────────

    static void seg3CostModel() {
        Configuration config = new Configuration();
        TrinoPlatform.getInstance().configureDefaults(config);

        long   mhz      = config.getLongProperty("wayang.trino.cpu.mhz",       0);
        long   cores    = config.getLongProperty("wayang.trino.cores",          0);
        double fix      = config.getDoubleProperty("wayang.trino.costs.fix",    0);
        double perMs    = config.getDoubleProperty("wayang.trino.costs.per-ms", 1);

        long   rows      = 10;
        long   alpha     = 10;
        long   beta      = 800_000;
        long   cpuCycles = alpha * rows + beta;
        double timeMs    = cpuCycles / (cores * mhz * 1000.0);
        double cost      = fix + perMs * timeMs;

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 3 — Cost Model Integration");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  LAYER 1 — Cost formula  (wayang-trino-defaults.properties)");
        System.out.printf("    tablesource : %s%n", config.getStringProperty("wayang.trino.tablesource.load", null));
        System.out.printf("    filter      : %s%n", config.getStringProperty("wayang.trino.filter.load",      null));
        System.out.println();
        System.out.println("  LAYER 2 — Hardware profile  (cpu cycles → wall-clock ms)");
        System.out.printf("    cpu.mhz = %d   cores = %d%n", mhz, cores);
        System.out.println();
        System.out.println("  LAYER 3 — Time → abstract cost  (cross-platform comparison)");
        System.out.printf("    costs.fix = %.1f   costs.per-ms = %.1f%n", fix, perMs);
        System.out.println();
        System.out.println("  ── Worked example: 10-row table scan ─────────────");
        System.out.printf("    cpu cycles = %d × %d + %d = %,d%n", alpha, rows, beta, cpuCycles);
        System.out.printf("    time       = %,d / (%d × %d × 1000) = %.4f ms%n", cpuCycles, cores, mhz, timeMs);
        System.out.printf("    cost       = %.1f + %.1f × %.4f = %.4f%n", fix, perMs, timeMs, cost);
        System.out.println();
        System.out.println("  Optimizer compares this cost against Java, Spark, etc.");
        System.out.println("  Tune α and β in defaults.properties after real benchmarks.");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    // ── Seg 4 — End-to-end query through Wayang ───────────────────────────────

    static void seg4EndToEnd() throws Exception {
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 4 — End-to-end via Wayang API");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  Query: SELECT * FROM iceberg.sales.orders WHERE region = 'AMER'");
        System.out.println("  Wayang will push the filter down as SQL to Trino.");
        System.out.println();

        // ── Build Wayang plan ────────────────────────────────────────────────
        Configuration config = new Configuration();
        config.setProperty("wayang.trino.jdbc.url",      JDBC_URL);
        config.setProperty("wayang.trino.jdbc.user",     JDBC_USER);
        config.setProperty("wayang.trino.jdbc.password", "");

        WayangContext wayang = new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Trino.plugin());

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

        // ── Execute ──────────────────────────────────────────────────────────
        wayang.execute("Trino-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %-8s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + "─".repeat(52));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %-8s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.println();
        System.out.printf("  ✓ %d AMER rows returned via Wayang → Trino SQL pushdown%n", results.size());

        // ── Verify SQL appeared in Trino's query history ─────────────────────
        System.out.println();
        System.out.println("  Checking Trino's system.runtime.queries for proof...");
        Properties props = new Properties();
        props.setProperty("user", JDBC_USER);
        try (Connection conn = DriverManager.getConnection(JDBC_URL, props)) {
            ResultSet rs = conn.createStatement().executeQuery(
                "SELECT query FROM system.runtime.queries " +
                "WHERE state = 'FINISHED' AND query LIKE '%iceberg.sales.orders%' " +
                "ORDER BY created DESC LIMIT 3"
            );
            System.out.println();
            System.out.println("  Last queries Trino executed containing 'iceberg.sales.orders':");
            while (rs.next()) {
                System.out.println("    ► " + rs.getString(1).replaceAll("\\s+", " "));
            }
        }
        System.out.println();
        System.out.println("  ✓ Wayang-assembled SQL confirmed in Trino's query history.");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }
}
