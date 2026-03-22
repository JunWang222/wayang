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

package org.apache.wayang.genericjdbc.trino;

import org.apache.wayang.genericjdbc.operators.GenericJdbcTableSource;

/**
 * Trino table source.
 *
 * <p>Table names use Trino's three-part naming convention:
 * {@code catalog.schema.table} (e.g. {@code iceberg.sales.orders}).
 *
 * <p>Overrides {@link #getPlatform()} to return {@link TrinoPlatform}, which
 * ensures:
 * <ul>
 *   <li>The operator is associated with Trino's SQL channel descriptor.</li>
 *   <li>Cost-model keys resolve to {@code wayang.trino.tablesource.load}.</li>
 *   <li>JDBC connection properties are read from {@code wayang.trino.jdbc.*}.</li>
 * </ul>
 */
public class TrinoTableSource extends GenericJdbcTableSource {

    public TrinoTableSource(String tableName, String... columnNames) {
        super("trino", tableName, columnNames);
    }

    protected TrinoTableSource(GenericJdbcTableSource that) {
        super(that);
    }

    @Override
    public TrinoPlatform getPlatform() {
        return TrinoPlatform.getInstance();
    }

    @Override
    protected TrinoTableSource createCopy() {
        return new TrinoTableSource(this);
    }
}
