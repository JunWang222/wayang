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

import org.apache.wayang.genericjdbc.operators.GenericJdbcTableSource;

/**
 * BigQuery table source.
 *
 * <p>Table names must use BigQuery's fully-qualified backtick-quoted form:
 * <pre>
 *   new BigQueryTableSource("`project.dataset.table`", "col1", "col2")
 * </pre>
 *
 * <p>Overrides {@link #getPlatform()} to return {@link BigQueryPlatform}, which
 * ensures:
 * <ul>
 *   <li>The operator is associated with BigQuery's SQL channel descriptor.</li>
 *   <li>Cost-model keys resolve to {@code wayang.bigquery.tablesource.load}.</li>
 *   <li>JDBC connection properties are read from {@code wayang.bigquery.jdbc.*}.</li>
 * </ul>
 */
public class BigQueryTableSource extends GenericJdbcTableSource {

    public BigQueryTableSource(String tableName, String... columnNames) {
        super("bigquery", tableName, columnNames);
    }

    protected BigQueryTableSource(GenericJdbcTableSource that) {
        super(that);
    }

    @Override
    public BigQueryPlatform getPlatform() {
        return BigQueryPlatform.getInstance();
    }

    @Override
    protected BigQueryTableSource createCopy() {
        return new BigQueryTableSource(this);
    }
}
