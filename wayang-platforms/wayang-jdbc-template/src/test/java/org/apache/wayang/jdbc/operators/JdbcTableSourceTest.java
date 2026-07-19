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

package org.apache.wayang.jdbc.operators;


import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.TableSink;
import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.apache.wayang.core.optimizer.cardinality.FixedSizeCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.jdbc.test.HsqldbFilterOperator;
import org.apache.wayang.jdbc.test.HsqldbPlatform;
import org.apache.wayang.jdbc.test.HsqldbTableSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link SqlToStreamOperator}.
 */
class JdbcTableSourceTest {

    @Test
    void testUserProvidedCardinalityEstimatorIsIgnoredByJdbcTableSource() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.hsqldb.cpu.mhz", "2700");
        configuration.setProperty("wayang.hsqldb.cores", "1");
        configuration.setProperty(
                "wayang.hsqldb.tablesource.load",
                "{\"in\":0,\"out\":1,\"cpu\":\"${1}\",\"ram\":\"0\",\"p\":1.0}"
        );
        configuration.setProperty(
                "wayang.hsqldb.filter.load",
                "{\"in\":1,\"out\":1,\"cpu\":\"${1}\",\"ram\":\"0\",\"p\":1.0}"
        );
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("DROP TABLE IF EXISTS testConfiguredCardinality;");
            statement.execute("CREATE TABLE testConfiguredCardinality (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testConfiguredCardinality VALUES (0, 'zero');");
            statement.execute("INSERT INTO testConfiguredCardinality VALUES (1, 'one');");
            statement.execute("INSERT INTO testConfiguredCardinality VALUES (2, 'two');");
        }

        JdbcTableSource tableSource = new HsqldbTableSource("testConfiguredCardinality");
        tableSource.setCardinalityEstimator(0, new FixedSizeCardinalityEstimator(100, true));
        System.out.println("[DEBUG JdbcTableSourceTest] User configured source cardinality estimator = 100.");

        HsqldbFilterOperator filter = new HsqldbFilterOperator(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> true,
                        Record.class
                ).withSqlImplementation("a >= 0")
        );
        TableSink<Record> sink = new TableSink<>(
                new Properties(),
                "overwrite",
                "testConfiguredCardinalitySink",
                "a",
                "b"
        );
        tableSource.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        WayangPlan wayangPlan = new WayangPlan(sink);
        wayangPlan.prepare();

        Job job = new WayangContext(configuration).createJob("debug configured cardinality", wayangPlan);
        DefaultOptimizationContext optimizationContext = DefaultOptimizationContext.createFrom(job);

        CardinalityEstimatorManager manager =
                new CardinalityEstimatorManager(wayangPlan, optimizationContext, job.getConfiguration());
        manager.pushCardinalities();

        CardinalityEstimate actualSourceCardinality =
                optimizationContext.getOperatorContext(tableSource).getOutputCardinality(0);
        System.out.println("[DEBUG JdbcTableSourceTest] Actual source cardinality after push = "
                + actualSourceCardinality);

        assertEquals(
                new CardinalityEstimate(3, 3, 1d),
                actualSourceCardinality
        );
    }

    @Test
    void testCardinalityEstimatorIsInvokedByCardinalityPush() throws SQLException {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.hsqldb.cpu.mhz", "2700");
        configuration.setProperty("wayang.hsqldb.cores", "1");
        configuration.setProperty(
                "wayang.hsqldb.tablesource.load",
                "{\"in\":0,\"out\":1,\"cpu\":\"${1}\",\"ram\":\"0\",\"p\":1.0}"
        );
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(configuration).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("DROP TABLE IF EXISTS testCardinalityPush;");
            statement.execute("CREATE TABLE testCardinalityPush (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testCardinalityPush VALUES (0, 'zero');");
            statement.execute("INSERT INTO testCardinalityPush VALUES (1, 'one');");
            statement.execute("INSERT INTO testCardinalityPush VALUES (2, 'two');");
        }

        JdbcTableSource tableSource = new HsqldbTableSource("testCardinalityPush");
        TableSink sink = new TableSink(new Properties(), "overwrite", "testCardinalityPushSink", "a", "b");
        tableSource.connectTo(0, sink, 0);

        WayangPlan wayangPlan = new WayangPlan(sink);
        wayangPlan.prepare();

        Job job = new WayangContext(configuration).createJob("debug cardinality push", wayangPlan);
        DefaultOptimizationContext optimizationContext = DefaultOptimizationContext.createFrom(job);

        CardinalityEstimatorManager manager =
                new CardinalityEstimatorManager(wayangPlan, optimizationContext, job.getConfiguration());
        manager.pushCardinalities();

        assertEquals(
                new CardinalityEstimate(3, 3, 1d),
                optimizationContext.getOperatorContext(tableSource).getOutputCardinality(0)
        );
    }

    @Test
    void testCardinalityEstimator() throws SQLException {
        Job job = mock(Job.class);
        DefaultOptimizationContext optimizationContext = mock(DefaultOptimizationContext.class);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        when(optimizationContext.getJob()).thenReturn(job);
        when(job.getStopWatch()).thenReturn(new StopWatch(new Experiment("mock", new Subject("mock", "mock"))));
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());
        when(job.getConfiguration()).thenReturn(new Configuration());
        HsqldbPlatform hsqldbPlatform = new HsqldbPlatform();

        // Create some test data.
        try (Connection jdbcConnection = hsqldbPlatform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection()) {
            final Statement statement = jdbcConnection.createStatement();
            statement.execute("CREATE TABLE testCardinalityEstimator (a INT, b VARCHAR(6));");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (0, 'zero');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (1, 'one');");
            statement.execute("INSERT INTO testCardinalityEstimator VALUES (2, 'two');");
        }

        JdbcTableSource tableSource = new HsqldbTableSource("testCardinalityEstimator");
        final CardinalityEstimator cardinalityEstimator = tableSource.getCardinalityEstimator(0);

        final CardinalityEstimate estimate = cardinalityEstimator.estimate(optimizationContext);

        assertEquals(
                new CardinalityEstimate(3, 3, 1d),
                estimate
        );
    }

}
