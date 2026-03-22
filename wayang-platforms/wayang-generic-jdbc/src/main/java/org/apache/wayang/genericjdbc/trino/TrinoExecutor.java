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

import org.apache.wayang.core.api.Job;
import org.apache.wayang.genericjdbc.execution.GenericJdbcExecutor;

/**
 * Executor for the {@link TrinoPlatform}.
 *
 * <p>Trino speaks standard ANSI SQL for the operators Wayang currently
 * pushes down (TableScan, Filter, Projection), so no SQL-dialect overrides
 * are needed beyond what {@link GenericJdbcExecutor} already produces.
 * This class exists as an extension point for future Trino-specific SQL
 * generation (e.g. TABLESAMPLE, connector-specific functions).
 */
public class TrinoExecutor extends GenericJdbcExecutor {

    public TrinoExecutor(TrinoPlatform platform, Job job) {
        super(platform, job);
    }
}
