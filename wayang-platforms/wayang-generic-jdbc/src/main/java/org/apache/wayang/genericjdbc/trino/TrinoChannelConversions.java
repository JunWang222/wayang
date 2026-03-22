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

import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.genericjdbc.operators.GenericSqlToStreamOperator;
import org.apache.wayang.java.channels.StreamChannel;

import java.util.Collection;
import java.util.Collections;

/**
 * Channel conversions for the {@link TrinoPlatform}.
 *
 * <p>The SQL-to-Stream conversion wraps {@link TrinoPlatform#getInstance()} so
 * that {@link GenericSqlToStreamOperator} resolves cost-model keys from
 * {@code wayang.trino.sqltostream.load.*} and opens the JDBC connection
 * via {@code wayang.trino.jdbc.*}.
 */
public class TrinoChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            TrinoPlatform.getInstance().getGenericSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new GenericSqlToStreamOperator(TrinoPlatform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Collections.singleton(
            SQL_TO_STREAM_CONVERSION
    );
}
