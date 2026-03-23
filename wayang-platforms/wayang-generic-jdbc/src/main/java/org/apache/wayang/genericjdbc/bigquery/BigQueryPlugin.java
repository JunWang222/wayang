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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.platform.JavaPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * Plugin that enables Wayang operators to run on {@link BigQueryPlatform}.
 *
 * <p>Registering this plugin:
 * <ul>
 *   <li>Declares {@link BigQueryPlatform} as a required platform, triggering
 *       the load of {@code wayang-bigquery-defaults.properties}.</li>
 *   <li>Provides BigQuery-specific {@link Mapping}s so Filter and Projection
 *       operators can be pushed down as SQL on BigQuery.</li>
 *   <li>Provides the SQL-to-Stream {@link ChannelConversion} that materialises
 *       BigQuery results into a Java {@code Stream}.</li>
 * </ul>
 *
 * <p>Typical usage:
 * <pre>{@code
 *   new WayangContext(config)
 *       .withPlugin(Java.basicPlugin())
 *       .withPlugin(BigQuery.plugin());
 * }</pre>
 */
public class BigQueryPlugin implements Plugin {

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(BigQueryPlatform.getInstance(), JavaPlatform.getInstance());
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Arrays.asList(
                new BigQueryFilterMapping(),
                new BigQueryProjectionMapping()
        );
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return BigQueryChannelConversions.ALL;
    }

    @Override
    public void setProperties(Configuration configuration) {
    }
}
