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

import org.apache.logging.log4j.LogManager;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * JDBC implementation for {@link ParquetSource}s exposed by an engine as a SQL
 * relation, such as a Hive/Iceberg table or BigQuery external table.
 */
public abstract class JdbcParquetSource extends ParquetSource implements JdbcSourceOperator {

    public JdbcParquetSource(String sourceName, String[] projection, String... columnNames) {
        super(sourceName, projection, columnNames);
    }

    public JdbcParquetSource(ParquetSource that) {
        super(that);
    }

    @Override
    public String getSourceName() {
        return this.getInputUrl();
    }

    @Override
    public String getSourceName(Configuration configuration) {
        return this.resolveSourceName(configuration);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        return this.getSourceName();
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler, Configuration configuration) {
        return this.getSourceName(configuration);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.parquetsource.load", this.getPlatform().getPlatformId());
    }

    @Override
    public org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator getCardinalityEstimator(int outputIndex) {
        assert outputIndex == 0;
        return new org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator() {
            @Override
            public CardinalityEstimate estimate(OptimizationContext optimizationContext,
                                                CardinalityEstimate... inputEstimates) {
                final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                        "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
                );

                try (Connection connection = JdbcParquetSource.this.getPlatform()
                        .createDatabaseDescriptor(optimizationContext.getConfiguration())
                        .createJdbcConnection()) {
                    final String sql = String.format("SELECT count(*) FROM %s",
                            JdbcParquetSource.this.getSourceName(optimizationContext.getConfiguration()));
                    final ResultSet resultSet = connection.createStatement().executeQuery(sql);
                    if (!resultSet.next()) {
                        throw new SQLException("No query result for \"" + sql + "\".");
                    }
                    long cardinality = resultSet.getLong(1);
                    return new CardinalityEstimate(cardinality, cardinality, 1d);
                } catch (Exception e) {
                    LogManager.getLogger(this.getClass()).error(
                            "Could not estimate cardinality for {}.", JdbcParquetSource.this, e
                    );
                    return new CardinalityEstimate(10, 10000000, 0.9);
                } finally {
                    timeMeasurement.stop();
                }
            }
        };
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            int outputIndex,
            Configuration configuration) {
        return Optional.of(this.getCardinalityEstimator(outputIndex));
    }

    private String resolveSourceName(Configuration configuration) {
        if (configuration == null) {
            return this.getSourceName();
        }

        final String platformId = this.getPlatform().getPlatformId();
        final String mappingKey = String.format("wayang.%s.parquetsource.mappings", platformId);
        final Optional<String> mapping = configuration.getOptionalStringProperty(mappingKey);
        if (mapping.isEmpty()) {
            return this.getSourceName();
        }

        final String inputUrl = this.getInputUrl();
        for (String entry : mapping.get().split(";")) {
            final String trimmedEntry = entry.trim();
            if (trimmedEntry.isEmpty()) {
                continue;
            }

            final int separator = trimmedEntry.indexOf('=');
            if (separator < 0) {
                LogManager.getLogger(this.getClass()).warn(
                        "Ignoring invalid Parquet source mapping entry '{}' for {}.", trimmedEntry, mappingKey
                );
                continue;
            }

            final String sourceUri = trimmedEntry.substring(0, separator).trim();
            final String relationName = trimmedEntry.substring(separator + 1).trim();
            if (sourceUri.equals(inputUrl) && !relationName.isEmpty()) {
                return relationName;
            }
        }

        return this.getSourceName();
    }
}
