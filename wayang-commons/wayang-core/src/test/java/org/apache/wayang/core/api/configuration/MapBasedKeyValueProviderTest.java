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

package org.apache.wayang.core.api.configuration;

import org.apache.wayang.core.api.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MapBasedKeyValueProvider} explicit tracking and caching behavior.
 */
public class MapBasedKeyValueProviderTest {

    @Test
    public void testExplicitSetAndProvide() {
        final MapBasedKeyValueProvider<String, String> provider = new MapBasedKeyValueProvider<>((KeyValueProvider<String, String>) null, false);
        Assertions.assertFalse(provider.isExplicitLocally("k1"));
        Assertions.assertNull(provider.provideExplicitLocally("k1"));
        Assertions.assertNull(provider.provideLocally("k1"));

        provider.set("k1", "v1");
        Assertions.assertTrue(provider.isExplicitLocally("k1"));
        Assertions.assertEquals("v1", provider.provideExplicitLocally("k1"));
        Assertions.assertEquals("v1", provider.provideLocally("k1"));
    }

    @Test
    public void testCachingDoesNotMarkKeyAsExplicit() {
        final MapBasedKeyValueProvider<String, String> parent = new MapBasedKeyValueProvider<>((KeyValueProvider<String, String>) null, false);
        parent.set("inheritedKey", "parentVal");

        final MapBasedKeyValueProvider<String, String> child = new MapBasedKeyValueProvider<>(parent, true);

        // Before read: not explicit and not present locally
        Assertions.assertFalse(child.isExplicitLocally("inheritedKey"));
        Assertions.assertNull(child.provideExplicitLocally("inheritedKey"));
        Assertions.assertNull(child.provideLocally("inheritedKey"));

        // Normal read causes caching into child
        final String resolved = child.provideFor("inheritedKey");
        Assertions.assertEquals("parentVal", resolved);

        // After caching: provideLocally returns cached value, but isExplicitLocally must remain FALSE
        Assertions.assertEquals("parentVal", child.provideLocally("inheritedKey"));
        Assertions.assertFalse(child.isExplicitLocally("inheritedKey"));
        Assertions.assertNull(child.provideExplicitLocally("inheritedKey"));
    }

    @Test
    public void testExplicitOverrideAfterCachedRead() {
        final MapBasedKeyValueProvider<String, String> parent = new MapBasedKeyValueProvider<>((KeyValueProvider<String, String>) null, false);
        parent.set("key", "parentVal");

        final MapBasedKeyValueProvider<String, String> child = new MapBasedKeyValueProvider<>(parent, true);
        child.provideFor("key"); // cache parentVal locally

        Assertions.assertFalse(child.isExplicitLocally("key"));
        Assertions.assertNull(child.provideExplicitLocally("key"));

        // Now explicitly set child
        child.set("key", "childVal");
        Assertions.assertTrue(child.isExplicitLocally("key"));
        Assertions.assertEquals("childVal", child.provideExplicitLocally("key"));
        Assertions.assertEquals("childVal", child.provideLocally("key"));
    }

    @Test
    public void testConfigurationExplicitPropertyTracking() {
        final Configuration parent = new Configuration();
        parent.setProperty("foo", "bar");

        final Configuration child = parent.fork();
        Assertions.assertFalse(child.isPropertyExplicitLocally("foo"));
        Assertions.assertNull(child.getExplicitLocalStringProperty("foo"));

        // Query property through child (triggers caching in MapBasedKeyValueProvider)
        Assertions.assertEquals("bar", child.getStringProperty("foo"));

        // Caching must not mark property as explicit locally
        Assertions.assertFalse(child.isPropertyExplicitLocally("foo"));
        Assertions.assertNull(child.getExplicitLocalStringProperty("foo"));

        // Explicitly setting property on child makes it explicit
        child.setProperty("foo", "baz");
        Assertions.assertTrue(child.isPropertyExplicitLocally("foo"));
        Assertions.assertEquals("baz", child.getExplicitLocalStringProperty("foo"));
    }

    @Test
    public void testRemoveProperty() {
        final Configuration parent = new Configuration();
        parent.setProperty("foo", "parentVal");

        final Configuration child = parent.fork();
        child.setProperty("foo", "childVal");
        Assertions.assertTrue(child.isPropertyExplicitLocally("foo"));
        Assertions.assertEquals("childVal", child.getStringProperty("foo"));

        child.removeProperty("foo");
        Assertions.assertFalse(child.isPropertyExplicitLocally("foo"));
        Assertions.assertEquals("parentVal", child.getStringProperty("foo"));
    }
}
