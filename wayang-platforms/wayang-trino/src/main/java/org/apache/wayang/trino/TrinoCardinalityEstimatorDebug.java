/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to You under the
 * Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License.  You may obtain a copy of
 * the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.wayang.trino;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.trino.operators.TrinoFilterOperator;
import org.apache.wayang.trino.operators.TrinoTableSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * Temporary debug runner for proving whether Trino table sources invoke
 * {@code JdbcTableSource#getCardinalityEstimator} during optimization.
 */
public class TrinoCardinalityEstimatorDebug {

    private static final String HOST = System.getenv().getOrDefault("TRINO_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("TRINO_PORT", "8080"));
    private static final String USER = System.getenv().getOrDefault("TRINO_USER", "admin");
    private static final String JDBC_URL = String.format("jdbc:trino://%s:%d", HOST, PORT);

    private static final String SCHEMA = "iceberg.wayang_debug";
    private static final String SOURCE_TABLE = SCHEMA + ".cardinality_source";
    private static final String SINK_TABLE = SCHEMA + ".cardinality_sink";
    private static final String[] COLUMNS = {"id", "name"};

    public static void main(String[] args) throws Exception {
        if (!isTrinoAvailable()) {
            System.out.println("[DEBUG TrinoCardinalityEstimator] Trino not reachable at " + JDBC_URL);
            return;
        }

        prepareTable();

        TrinoTableSource source = new TrinoTableSource(SOURCE_TABLE, COLUMNS);
        source.setCardinalityEstimator(0, new FixedSizeCardinalityEstimator(100, true));
        System.out.println("[DEBUG TrinoCardinalityEstimator] User configured source cardinality estimator = 100.");

        TrinoFilterOperator filter = new TrinoFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> true,
                        Record.class
                ).withSqlImplementation("id >= 0")
        );
        TableSink<Record> sink = new TableSink<>(new Properties(), "overwrite", SINK_TABLE, COLUMNS);
        source.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.trino.jdbc.url", JDBC_URL);
        configuration.setProperty("wayang.trino.jdbc.user", USER);
        configuration.setProperty("wayang.trino.jdbc.password", "");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Trino.plugin());
        wayangContext.getConfiguration().getPlatformProvider().addToBlacklist(JavaPlatform.getInstance());

        System.out.println("[DEBUG TrinoCardinalityEstimator] Building Trino-only initial execution plan.");
        wayangContext.buildInitialExecutionPlan(
                "debug trino-only cardinality estimator",
                new WayangPlan(sink)
        );
    }

    private static void prepareTable() throws Exception {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + SCHEMA);
            statement.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
            statement.execute("DROP TABLE IF EXISTS " + SINK_TABLE);
            statement.execute("CREATE TABLE " + SOURCE_TABLE + " (id BIGINT, name VARCHAR) WITH (format = 'PARQUET')");
            statement.execute("INSERT INTO " + SOURCE_TABLE + " VALUES (1, 'one'), (2, 'two'), (3, 'three')");
        }
    }

    private static boolean isTrinoAvailable() {
        try (Connection connection = jdbc(); Statement statement = connection.createStatement()) {
            statement.execute("SELECT 1");
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static Connection jdbc() throws Exception {
        return DriverManager.getConnection(JDBC_URL, USER, "");
    }
}
