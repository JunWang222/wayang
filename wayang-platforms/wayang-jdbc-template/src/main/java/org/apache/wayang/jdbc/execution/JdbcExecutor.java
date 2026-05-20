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

package org.apache.wayang.jdbc.execution;

import org.apache.wayang.basic.channels.FileChannel;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.ExecutorTemplate;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.jdbc.channels.SqlQueryChannel;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;
import org.apache.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.wayang.jdbc.operators.JdbcJoinOperator;
import org.apache.wayang.jdbc.operators.JdbcProjectionOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSinkOperator;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link Executor} implementation for the {@link JdbcPlatformTemplate}.
 */
public class JdbcExecutor extends ExecutorTemplate {

    private final JdbcPlatformTemplate platform;

    private final Connection connection;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final FunctionCompiler functionCompiler = new FunctionCompiler();

    public JdbcExecutor(final JdbcPlatformTemplate platform, final Job job) {
        super(job.getCrossPlatformExecutor());
        this.platform = platform;
        this.connection = this.platform.createDatabaseDescriptor(job.getConfiguration()).createJdbcConnection();
    }

    @Override
    public void execute(final ExecutionStage stage, final OptimizationContext optimizationContext, final ExecutionState executionState) {
        // Check if this stage ends with a sink operator
        final Collection<?> termTasks = stage.getTerminalTasks();
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];

        if (termTask.getOperator() instanceof JdbcTableSinkOperator) {
            // If it is a sink stage: compose and execute SQL directly within the database
            JdbcExecutor.executeSinkStage(stage, optimizationContext, this);
        } else {
            //If it is normal stage: compose SQL and store in channel for downstream consumption
            final Tuple2<String, SqlQueryChannel.Instance> pair = JdbcExecutor.createSqlQuery(stage, optimizationContext, this);
            final String query = pair.field0;
            final SqlQueryChannel.Instance queryChannel = pair.field1;
            queryChannel.setSqlQuery(query);
            executionState.register(queryChannel);
        }
    }

    /**
     * Handles execution stages that end with a {@link JdbcTableSinkOperator}.
     * Composes a SQL query from the stage's operators and executes it directly
     * on the database connection, keeping all data within the database.
     *
     * @param stage               the execution stage ending with a sink
     * @param optimizationContext provides optimization information
     * @param jdbcExecutor        the executor with the database connection
     */
    private static void executeSinkStage(final ExecutionStage stage,
                                         final OptimizationContext optimizationContext,
                                         final JdbcExecutor jdbcExecutor) {
        final Collection<?> startTasks = stage.getStartTasks();
        final Collection<?> termTasks = stage.getTerminalTasks();

        assert startTasks.size() == 1 : "Invalid JDBC stage: multiple sources are not currently supported";
        final ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        assert startTask.getOperator() instanceof TableSource
                : "Invalid JDBC stage: Start task has to be a TableSource";
        assert termTask.getOperator() instanceof JdbcTableSinkOperator
                : "Invalid JDBC stage: Terminal task has to be a JdbcTableSinkOperator";

        // Extract operators from the stage
        final JdbcTableSource tableOp = (JdbcTableSource) startTask.getOperator();
        final JdbcTableSinkOperator sinkOp = (JdbcTableSinkOperator) termTask.getOperator();
        final Collection<JdbcExecutionOperator> filterTasks = new ArrayList<>(4);
        JdbcProjectionOperator projectionTask = null;
        final Collection<JdbcExecutionOperator> joinTasks = new ArrayList<>();

        // Walk through intermediate operators, stopping at the sink
        ExecutionTask nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null && !(nextTask.getOperator() instanceof JdbcTableSinkOperator)) {
            if (nextTask.getOperator() instanceof final JdbcFilterOperator filterOperator) {
                filterTasks.add(filterOperator);
            } else if (nextTask.getOperator() instanceof JdbcProjectionOperator projectionOperator) {
                assert projectionTask == null;
                projectionTask = projectionOperator;
            } else if (nextTask.getOperator() instanceof JdbcJoinOperator joinOperator) {
                joinTasks.add(joinOperator);
            } else {
                throw new WayangException(String.format("Unsupported JDBC execution task %s", nextTask.toString()));
            }
            nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        // Compose the SELECT query
        final StringBuilder selectQuery = createSqlString(jdbcExecutor, tableOp, filterTasks, projectionTask, joinTasks);

        // Remove trailing semicolon from SELECT
        String selectSql = selectQuery.toString();
        if (selectSql.endsWith(";")) {
            selectSql = selectSql.substring(0, selectSql.length() - 1);
        }

        // Get the sink's SQL clause
        final String sinkClause = sinkOp.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);

        // Execute on the database
        try (Statement stmt = jdbcExecutor.connection.createStatement()) {
            // Handle overwrite: drop existing table first
            if ("overwrite".equals(sinkOp.getMode())) {
                stmt.execute("DROP TABLE IF EXISTS " + sinkOp.getTableName());
            }
            // Execute the composed query: CREATE TABLE x AS SELECT ... or INSERT INTO x SELECT ...
            final String fullSql = sinkClause + " " + selectSql + sinkOp.createSqlSuffix();
            stmt.execute(fullSql);
            jdbcExecutor.logger.info("Executed SQL sink: {}", fullSql);
        } catch (SQLException e) {
            throw new WayangException("Failed to execute SQL sink on table: " + sinkOp.getTableName(), e);
        }
    }

    /**
     * Retrieves the follow-up {@link ExecutionTask} of the given {@code task}
     * unless it is not comprising a
     * {@link JdbcExecutionOperator} and/or not in the given {@link ExecutionStage}.
     *
     * @param task  whose follow-up {@link ExecutionTask} is requested; should have
     *              a single follower
     * @param stage in which the follow-up {@link ExecutionTask} should be
     * @return the said follow-up {@link ExecutionTask} or {@code null} if none
     */
    private static ExecutionTask findJdbcExecutionOperatorTaskInStage(final ExecutionTask task, final ExecutionStage stage) {
        assert task.getNumOuputChannels() == 1;
        final Channel outputChannel = task.getOutputChannel(0);
        final ExecutionTask consumer = WayangCollections.getSingle(outputChannel.getConsumers());
        return consumer.getStage() == stage && consumer.getOperator() instanceof JdbcExecutionOperator ? consumer
                : null;
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                whose outbound {@link SqlQueryChannel} should be
     *                            instantiated
     * @param optimizationContext provides information about the
     *                            {@link ExecutionTask}
     * @return the {@link SqlQueryChannel.Instance}
     */
    private static SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
            final OptimizationContext optimizationContext, final JdbcExecutor jdbcExecutor) {
        assert task.getNumOuputChannels() == 1 : String.format("Illegal task: %s.", task);
        assert task.getOutputChannel(0) instanceof SqlQueryChannel : String.format("Illegal task: %s.", task);

        final SqlQueryChannel outputChannel = (SqlQueryChannel) task.getOutputChannel(0);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext
                .getOperatorContext(task.getOperator());
        return outputChannel.createInstance(jdbcExecutor, operatorContext, 0);
    }

    /**
     * Instantiates the outbound {@link SqlQueryChannel} of an
     * {@link ExecutionTask}.
     *
     * @param task                       whose outbound {@link SqlQueryChannel}
     *                                   should be instantiated
     * @param optimizationContext        provides information about the
     *                                   {@link ExecutionTask}
     * @param predecessorChannelInstance preceeding {@link SqlQueryChannel.Instance}
     *                                   to keep track of lineage
     * @return the {@link SqlQueryChannel.Instance}
     */
    private static SqlQueryChannel.Instance instantiateOutboundChannel(final ExecutionTask task,
            final OptimizationContext optimizationContext,
            final SqlQueryChannel.Instance predecessorChannelInstance, final JdbcExecutor jdbcExecutor) {
        final SqlQueryChannel.Instance newInstance = JdbcExecutor.instantiateOutboundChannel(task, optimizationContext, jdbcExecutor);
        newInstance.getLineage().addPredecessor(predecessorChannelInstance.getLineage());
        return newInstance;
    }

    /**
     * Creates a query channel and the sql statement
     * 
     * @param stage
     * @param context
     * @return a tuple containing the sql statement
     */
    protected static Tuple2<String, SqlQueryChannel.Instance> createSqlQuery(final ExecutionStage stage,
            final OptimizationContext context, final JdbcExecutor jdbcExecutor) {
        final Collection<?> startTasks = stage.getStartTasks();
        final Collection<?> termTasks = stage.getTerminalTasks();

        // Verify that we can handle this instance.
        assert startTasks.size() == 1 : "Invalid jdbc stage: multiple sources are not currently supported";
        final ExecutionTask startTask = (ExecutionTask) startTasks.toArray()[0];
        assert termTasks.size() == 1 : "Invalid JDBC stage: multiple terminal tasks are not currently supported.";
        final ExecutionTask termTask = (ExecutionTask) termTasks.toArray()[0];
        assert startTask.getOperator() instanceof TableSource
                : "Invalid JDBC stage: Start task has to be a TableSource";

        // Extract the different types of ExecutionOperators from the stage.
        final JdbcTableSource tableOp = (JdbcTableSource) startTask.getOperator();
        SqlQueryChannel.Instance tipChannelInstance = JdbcExecutor.instantiateOutboundChannel(startTask, context, jdbcExecutor);
        final Collection<JdbcExecutionOperator> filterTasks = new ArrayList<>(4);
        JdbcProjectionOperator projectionTask = null;
        final Collection<JdbcExecutionOperator> joinTasks = new ArrayList<>();
        final Set<ExecutionTask> allTasks = stage.getAllTasks();
        assert allTasks.size() <= 3;
        ExecutionTask nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(startTask, stage);
        while (nextTask != null) {
            // Evaluate the nextTask.
            final var operator = nextTask.getOperator();
            if (operator instanceof FilterOperator || operator instanceof SpatialFilterOperator) {
                filterTasks.add((JdbcExecutionOperator) operator);
            } else if (operator instanceof JdbcProjectionOperator) {
                assert projectionTask == null; // Allow one projection operator per stage for now.
                projectionTask = (JdbcProjectionOperator) operator;
            } else if (operator instanceof JoinOperator || (operator instanceof SpatialJoinOperator)) {
                joinTasks.add((JdbcExecutionOperator) operator);
            } else {
                throw new WayangException(String.format("Unsupported JDBC execution task %s", nextTask.toString()));
            }

            // Move the tipChannelInstance.
            tipChannelInstance = JdbcExecutor.instantiateOutboundChannel(nextTask, context, tipChannelInstance, jdbcExecutor);

            // Go to the next nextTask.
            nextTask = JdbcExecutor.findJdbcExecutionOperatorTaskInStage(nextTask, stage);
        }

        // Create the SQL query.
        final StringBuilder query = createSqlString(jdbcExecutor, tableOp, filterTasks, projectionTask, joinTasks);
        return new Tuple2<>(query.toString(), tipChannelInstance);
    }

    public static StringBuilder createSqlString(final JdbcExecutor jdbcExecutor, final JdbcTableSource tableOp,
            final Collection<JdbcExecutionOperator> filterTasks,
            JdbcProjectionOperator projectionTask,
            final Collection<JdbcExecutionOperator> joinTasks) {
        final String tableName = tableOp.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);
        final Collection<String> conditions = filterTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                .collect(Collectors.toList());
        final String projection = projectionTask == null ? "*" : projectionTask.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler);
        final Collection<String> joins = joinTasks.stream()
                .map(op -> op.createSqlClause(jdbcExecutor.connection, jdbcExecutor.functionCompiler))
                .collect(Collectors.toList());

        final StringBuilder sb = new StringBuilder(1000);
        sb.append("SELECT ").append(projection).append(" FROM ").append(tableName);
        if (!joins.isEmpty()) {
            final String separator = " ";
            for (final String join : joins) {
                sb.append(separator).append(join);
            }
        }
        if (!conditions.isEmpty()) {
            sb.append(" WHERE ");
            sb.append(String.join(" AND ", conditions));
        }
        sb.append(';');
        return sb;
    }

    @Override
    public void dispose() {
        try {
            this.connection.close();
        } catch (final SQLException e) {
            this.logger.error("Could not close JDBC connection to PostgreSQL correctly.", e);
        }
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private void saveResult(final FileChannel.Instance outputFileChannelInstance, final ResultSet rs)
            throws IOException, SQLException {
        // Output results.
        final FileSystem outFs = FileSystems.getFileSystem(outputFileChannelInstance.getSinglePath()).get();
        try (final OutputStreamWriter writer = new OutputStreamWriter(
                outFs.create(outputFileChannelInstance.getSinglePath()))) {
            while (rs.next()) {
                // System.out.println(rs.getInt(1) + " " + rs.getString(2));
                final ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    writer.write(rs.getString(i));
                    if (i < rsmd.getColumnCount()) {
                        writer.write('\t');
                    }
                }
                if (!rs.isLast()) {
                    writer.write('\n');
                }
            }
        } catch (final UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
