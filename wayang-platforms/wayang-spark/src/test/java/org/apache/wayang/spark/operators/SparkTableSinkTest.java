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
package org.apache.wayang.spark.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spark.channels.RddChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for {@link SparkTableSink}.
 */
class SparkTableSinkTest extends SparkOperatorTestBase {

    private static final String JDBC_URL = "jdbc:h2:mem:sparktestdb;DB_CLOSE_DELAY=-1";
    private static final String DRIVER = "org.h2.Driver";
    private static final String TABLE_NAME = "spark_test_table";

    private Connection connection;

    @BeforeEach
    void setupTest() throws Exception {
        Class.forName(DRIVER);
        connection = DriverManager.getConnection(JDBC_URL, "sa", "");
    }

    @AfterEach
    void teardownTest() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS \"" + TABLE_NAME + "\"");
            }
            connection.close();
        }
    }

    @Test
    void testWritingRecordToH2() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        SparkTableSink<Record> sink = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name", "value" },
                DataSetType.createDefault(Record.class));

        Record record1 = new Record(1, "Alice", 100.5);
        Record record2 = new Record(2, "Bob", 200.75);

        RddChannel.Instance inputChannelInstance = this.createRddChannelInstance(
                Arrays.asList(record1, record2));

        evaluate(sink, new ChannelInstance[] { inputChannelInstance }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testWritingPojoToH2() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        SparkTableSink<TestPojo> sink = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                null,
                DataSetType.createDefault(TestPojo.class));

        TestPojo p1 = new TestPojo(1, "Alice");
        TestPojo p2 = new TestPojo(2, "Bob");

        RddChannel.Instance inputChannelInstance = this.createRddChannelInstance(
                Arrays.asList(p1, p2));

        evaluate(sink, new ChannelInstance[] { inputChannelInstance }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\" ORDER BY \"id\"")) {
            rs.next();
            assertEquals(1, rs.getInt("id"));
            assertEquals("Alice", rs.getString("name"));
            rs.next();
            assertEquals(2, rs.getInt("id"));
            assertEquals("Bob", rs.getString("name"));
        }
    }

    @Test
    void testAppendMode() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        // 1. Initial write
        SparkTableSink<Record> sink1 = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(new Record(1, "Alice")));
        evaluate(sink1, new ChannelInstance[] { input1 }, new ChannelInstance[0]);

        // 2. Append write
        SparkTableSink<Record> sink2 = new SparkTableSink<>(dbProps, "append", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        RddChannel.Instance input2 = this.createRddChannelInstance(Arrays.asList(new Record(2, "Bob")));
        evaluate(sink2, new ChannelInstance[] { input2 }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testOverwriteWithSchemaMismatch() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        // 1. Create table with old schema
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (\"id\" INT, \"name\" VARCHAR(255))");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 'Old')");
        }

        // 2. Overwrite with new schema
        SparkTableSink<Record> sink = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "age", "city" },
                DataSetType.createDefault(Record.class));

        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(new Record(2, 30, "Berlin")));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\"")) {
            rs.next();
            assertEquals(2, rs.getInt("id"));
            assertEquals(30, rs.getInt("age"));
            assertEquals("Berlin", rs.getString("city"));

            // Verify 'name' column is gone
            boolean hasName = false;
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                if ("name".equalsIgnoreCase(rs.getMetaData().getColumnName(i))) {
                    hasName = true;
                }
            }
            assertFalse(hasName, "Column 'name' should have been dropped");
        }
    }

    @Test
    void testNullValues() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        SparkTableSink<Record> sink = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "name" },
                DataSetType.createDefault(Record.class));

        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(new Record(1, null)));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT \"name\" FROM \"" + TABLE_NAME + "\" WHERE \"id\" = 1")) {
            rs.next();
            assertEquals(null, rs.getString(1));
            assertTrue(rs.wasNull());
        }
    }

    @Test
    void testSupportedTypes() throws Exception {
        Properties dbProps = new Properties();
        dbProps.setProperty("url", JDBC_URL);
        dbProps.setProperty("user", "sa");
        dbProps.setProperty("password", "");
        dbProps.setProperty("driver", DRIVER);

        SparkTableSink<Record> sink = new SparkTableSink<>(dbProps, "overwrite", TABLE_NAME,
                new String[] { "id", "is_active", "salary", "score" },
                DataSetType.createDefault(Record.class));

        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(new Record(1, true, 5000.50, 95.5f)));
        evaluate(sink, new ChannelInstance[] { input }, new ChannelInstance[0]);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM \"" + TABLE_NAME + "\" WHERE \"id\" = 1")) {
            rs.next();
            assertTrue(rs.getBoolean("is_active"));
            assertEquals(5000.50, rs.getDouble("salary"), 0.001);
            assertEquals(95.5f, rs.getFloat("score"), 0.001f);
        }
    }

    public static class TestPojo implements java.io.Serializable {
        private int id;
        private String name;

        public TestPojo() {
        }

        public TestPojo(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}