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

package org.apache.wayang.genericjdbc;

import org.apache.wayang.genericjdbc.trino.TrinoPlatform;
import org.apache.wayang.genericjdbc.trino.TrinoPlugin;

/**
 * Entry point for the Trino connector.
 *
 * <p>Typical usage:
 * <pre>{@code
 *   new WayangContext(config)
 *       .withPlugin(Java.basicPlugin())
 *       .withPlugin(Trino.plugin());
 * }</pre>
 */
public class Trino {

    private static final TrinoPlugin PLUGIN = new TrinoPlugin();

    public static TrinoPlugin plugin() {
        return PLUGIN;
    }

    public static TrinoPlatform platform() {
        return TrinoPlatform.getInstance();
    }
}
