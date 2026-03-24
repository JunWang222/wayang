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

package org.apache.wayang.genericjdbc.bigquery;

import org.apache.wayang.core.api.Job;
import org.apache.wayang.genericjdbc.execution.GenericJdbcExecutor;

import java.util.Collection;

/**
 * Executor for the {@link BigQueryPlatform}.
 *
 * <h3>BigQuery SQL dialect notes</h3>
 * <ul>
 *   <li><b>No trailing semicolon</b> — already removed in the base class.</li>
 *   <li><b>Table names</b> — must be backtick-quoted: {@code `project.dataset.table`}.
 *       Users pass the backtick-quoted name as the {@code tableName} constructor
 *       argument, so no extra quoting is needed here.</li>
 *   <li><b>Column names</b> — simple identifiers (e.g. {@code region}, {@code amount})
 *       work without quoting. Override {@link #quoteIdentifier} if reserved-word
 *       columns need backtick wrapping in the SELECT list.</li>
 *   <li><b>WHERE conditions</b> — passed as raw SQL strings via
 *       {@code withSqlImplementation()}; no rewriting needed.</li>
 * </ul>
 */
public class BigQueryExecutor extends GenericJdbcExecutor {

    public BigQueryExecutor(BigQueryPlatform platform, Job job) {
        super(platform, job);
    }

    /**
     * Wraps an identifier in BigQuery-style backtick quotes.
     *
     * <p>Call this from a future override of {@link #createSqlQuery} if column
     * names passed to projection need escaping (e.g. they are BigQuery reserved
     * words like {@code date}, {@code time}, {@code timestamp}).
     */
    protected String quoteIdentifier(String name) {
        return "`" + name + "`";
    }

    /**
     * BigQuery SQL generation.
     *
     * <p>Currently delegates to the base class (standard ANSI SQL), which
     * produces queries compatible with BigQuery for simple identifiers.
     * Override here when BigQuery-specific syntax is needed (TABLESAMPLE,
     * LIMIT x OFFSET y, backtick column quoting, etc.).
     */
    @Override
    protected String createSqlQuery(String tableName, Collection<String> conditions, String projection) {
        return super.createSqlQuery(tableName, conditions, projection);
    }
}
