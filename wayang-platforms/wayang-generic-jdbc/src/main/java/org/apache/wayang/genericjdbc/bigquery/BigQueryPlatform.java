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

import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;

/**
 * BigQuery-specific {@link GenericJdbcPlatform} with {@code CONFIG_NAME = "bigquery"}.
 *
 * <p>Setting {@code CONFIG_NAME = "bigquery"} causes Wayang to resolve all
 * property keys with that prefix automatically:
 * <ul>
 *   <li>Connection: {@code wayang.bigquery.jdbc.url} (full JDBC URL including OAuth params),
 *       {@code .user}, {@code .password}, {@code .driverName}</li>
 *   <li>Cost model: {@code wayang.bigquery.tablesource.load},
 *       {@code .sqltostream.load.query}, …</li>
 *   <li>Hardware: {@code wayang.bigquery.cpu.mhz}, {@code .cores}, {@code .costs.*}</li>
 * </ul>
 *
 * <p>Default values are loaded from {@code wayang-bigquery-defaults.properties}
 * on the classpath when this platform is initialised.
 *
 * <p>BigQuery JDBC URL format:
 * <pre>
 *   jdbc:bigquery://https://www.googleapis.com/bigquery/v2;
 *     ProjectId=my-project;
 *     OAuthType=0;
 *     OAuthServiceAcctEmail=sa@my-project.iam.gserviceaccount.com;
 *     OAuthPvtKeyPath=/path/to/key.json
 * </pre>
 *
 * <p>Table names must be backtick-quoted:
 * {@code `project.dataset.table`}
 */
public class BigQueryPlatform extends GenericJdbcPlatform {

    private static final String PLATFORM_NAME = "BigQuery";
    private static final String CONFIG_NAME   = "bigquery";

    private static BigQueryPlatform instance;

    public static BigQueryPlatform getInstance() {
        if (instance == null) {
            instance = new BigQueryPlatform();
        }
        return instance;
    }

    protected BigQueryPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return job -> new BigQueryExecutor(this, job);
    }

    @Override
    public String getJdbcDriverClassName() {
        return "com.google.cloud.bigquery.jdbc.BigQueryDriver";
    }
}
