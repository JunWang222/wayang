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
import org.apache.wayang.genericjdbc.bigquery.BigQueryPlatform;
import org.apache.wayang.genericjdbc.bigquery.BigQueryTableSource;
import org.apache.wayang.java.Java;

import java.util.ArrayList;
import java.util.List;

/**
 * Standalone demo for the Wayang BigQuery connector.
 *
 * <p>Run with:
 * <pre>
 *   cd /path/to/wayang
 *   mvn exec:java -pl wayang-platforms/wayang-generic-jdbc \
 *     -Dexec.mainClass=org.apache.wayang.genericjdbc.BigQueryDemo \
 *     -Pskip-prerequisite-check -Drat.skip=true \
 *     [-Dbigquery.url=jdbc:bigquery://...] \
 *     [-Dbigquery.project=my-project]
 * </pre>
 *
 * <p>Demonstrates:
 * <ol>
 *   <li>Seg 3 — Cost model: shows the three-layer pipeline
 *       (formula → cpu cycles → time → abstract cost) with BigQuery's
 *       α=5, β=2M parameters (serverless columnar engine profile).</li>
 *   <li>Seg 4 — End-to-end plan: builds the Wayang plan with filter pushdown.
 *       Executes if {@code bigquery.url} is provided; otherwise prints the
 *       plan structure and explains what would be sent to BigQuery.</li>
 * </ol>
 */
public class BigQueryDemo {

    private static final String JDBC_URL = System.getProperty("bigquery.url",  "");
    private static final String PROJECT  = System.getProperty("bigquery.project", "my-project");

    public static void main(String[] args) {
        seg3CostModel();
        seg4PlanAndRun();
    }

    // ── Seg 3 — Cost model ────────────────────────────────────────────────────

    static void seg3CostModel() {
        Configuration config = new Configuration();
        BigQueryPlatform.getInstance().configureDefaults(config);

        long   mhz      = config.getLongProperty("wayang.bigquery.cpu.mhz",       0);
        long   cores    = config.getLongProperty("wayang.bigquery.cores",          0);
        double fix      = config.getDoubleProperty("wayang.bigquery.costs.fix",    0);
        double perMs    = config.getDoubleProperty("wayang.bigquery.costs.per-ms", 1);

        long   rows      = 10;
        long   alpha     = 5;
        long   beta      = 2_000_000;
        long   cpuCycles = alpha * rows + beta;
        double timeMs    = cpuCycles / (cores * mhz * 1000.0);
        double cost      = fix + perMs * timeMs;

        System.out.println();
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 3 — BigQuery Cost Model Integration");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
        System.out.println("  LAYER 1 — Cost formula  (wayang-bigquery-defaults.properties)");
        System.out.printf("    tablesource : %s%n", config.getStringProperty("wayang.bigquery.tablesource.load", null));
        System.out.printf("    filter      : %s%n", config.getStringProperty("wayang.bigquery.filter.load",      null));
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
        System.out.println("  Compare vs Trino: α=10, β=800k, cost≈0.0741");
        System.out.println("  BigQuery: α=5 (serverless columnar, cheaper per-row)");
        System.out.println("            β=2M (larger cold-start / slot reservation)");
        System.out.println("  ─── optimizer picks Trino for small tables, BigQuery for large ones");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }

    // ── Seg 4 — Wayang plan with filter pushdown ──────────────────────────────

    static void seg4PlanAndRun() {
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println("  Seg 4 — BigQuery via Wayang API");
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();

        String table = String.format("`%s.sales.orders`", PROJECT);

        System.out.println("  Plan structure (same API as Trino, different platform):");
        System.out.println();
        System.out.printf("    new BigQueryTableSource(\"%s\", ...)%n", table);
        System.out.println("        │");
        System.out.println("        ▼  BigQueryFilterMapping rewrites →");
        System.out.println("    FilterOperator(region = 'AMER')  →  BigQueryFilterOperator");
        System.out.println("        │");
        System.out.println("        ▼  TrinoChannelConversion: SqlQueryChannel → StreamChannel");
        System.out.println("    LocalCallbackSink (collect results in Java)");
        System.out.println();
        System.out.println("  SQL Wayang would send to BigQuery:");
        System.out.printf("    SELECT * FROM %s WHERE region = 'AMER'%n", table);
        System.out.println();
        System.out.println("  Note: BigQuery uses backtick-quoted `project.dataset.table`");
        System.out.println("        identifiers — handled automatically by BigQueryTableSource.");
        System.out.println();

        if (JDBC_URL.isEmpty()) {
            System.out.println("  ── Live run skipped (no -Dbigquery.url provided) ─────");
            System.out.println("  To run against real BigQuery set:");
            System.out.println("    -Dbigquery.url=\"jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443\"");
            System.out.println("                  \";ProjectId=my-project\"");
            System.out.println("                  \";OAuthType=0\"");
            System.out.println("                  \";OAuthServiceAcctEmail=sa@project.iam.gserviceaccount.com\"");
            System.out.println("                  \";OAuthPvtKeyPath=/path/to/key.json\"");
            System.out.println("  ──────────────────────────────────────────────────────");
            System.out.println("══════════════════════════════════════════════════════");
            System.out.println();
            return;
        }

        // ── Live execution (only when JDBC URL is provided) ──────────────────
        Configuration config = new Configuration();
        config.setProperty("wayang.bigquery.jdbc.url", JDBC_URL);

        WayangContext wayang = new WayangContext(config)
                .withPlugin(Java.basicPlugin())
                .withPlugin(BigQuery.plugin());

        List<Record> results = new ArrayList<>();
        BigQueryTableSource source = new BigQueryTableSource(
                table, "order_id", "region", "product", "amount", "order_date"
        );
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        r -> "AMER".equals(r.getField(1)), Record.class
                ).withSqlImplementation("region = 'AMER'")
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(results, Record.class);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        wayang.execute("BigQuery-Demo", new WayangPlan(sink));

        System.out.println("  Results returned by Wayang:");
        System.out.printf("  %-10s %-6s %-10s %-8s %-12s%n",
                "order_id", "region", "product", "amount", "order_date");
        System.out.println("  " + "─".repeat(52));
        for (Record r : results) {
            System.out.printf("  %-10s %-6s %-10s %-8s %-12s%n",
                    r.getField(0), r.getField(1), r.getField(2), r.getField(3), r.getField(4));
        }
        System.out.printf("%n  ✓ %d AMER rows returned via Wayang → BigQuery SQL pushdown%n", results.size());
        System.out.println("══════════════════════════════════════════════════════");
        System.out.println();
    }
}
