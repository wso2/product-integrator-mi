/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.micro.integrator.management.apis;

import org.apache.commons.lang3.math.Fraction;
import org.apache.synapse.libraries.model.SynapseLibrary;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.connector.core.ConnectionHealthRegistry;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ConnectorResourceHealthTest {

    private static final String CONNECTOR = "file";
    private static final String STANDARD_PACKAGE = "org.wso2.carbon.connector";

    private final Map<String, Supplier<Map<String, Object>>> registered = new HashMap<>();

    @After
    public void clear() {
        registered.forEach((connection, health) -> ConnectionHealthRegistry.unregister(CONNECTOR, connection, health));
    }

    @Test
    public void testConnectionsInNameOrderWithStatusAndHealth() throws Exception {
        register("zeta", health("breaker_open", 2L));
        register("alpha", health("active", 5L));

        for (JSONObject entry : new JSONObject[]{listEntry(), singleEntry()}) {
            JSONArray connections = entry.getJSONArray("connections");
            Assert.assertEquals(2, connections.length());

            JSONObject first = connections.getJSONObject(0);
            Assert.assertEquals("alpha", first.getString("name"));
            Assert.assertEquals("active", first.getString("status"));
            Assert.assertEquals(5L, first.getJSONObject("health").getLong("filesDone"));
            Assert.assertEquals("active", first.getJSONObject("health").getString("status"));

            JSONObject second = connections.getJSONObject(1);
            Assert.assertEquals("zeta", second.getString("name"));
            Assert.assertEquals("breaker_open", second.getString("status"));
            Assert.assertEquals(2L, second.getJSONObject("health").getLong("filesDone"));
        }
    }

    @Test
    public void testSupplierThatThrowsGivesTheNameOnly() throws Exception {
        register("c1", () -> {
            throw new IllegalStateException("health failed");
        });

        assertNameOnly(listEntry().getJSONArray("connections").getJSONObject(0), "c1");
        assertNameOnly(singleEntry().getJSONArray("connections").getJSONObject(0), "c1");
    }

    @Test
    public void testSupplierThatReturnsNullGivesTheNameOnly() throws Exception {
        register("c1", () -> null);

        assertNameOnly(listEntry().getJSONArray("connections").getJSONObject(0), "c1");
        assertNameOnly(singleEntry().getJSONArray("connections").getJSONObject(0), "c1");
    }

    @Test
    public void testHealthWithNoStatusGivesNoStatus() throws Exception {
        Map<String, Object> health = new HashMap<>();
        health.put("filesDone", 1L);
        register("c1", () -> health);

        JSONObject connection = listEntry().getJSONArray("connections").getJSONObject(0);
        Assert.assertEquals("c1", connection.getString("name"));
        Assert.assertFalse(connection.has("status"));
        Assert.assertEquals(1L, connection.getJSONObject("health").getLong("filesDone"));
    }

    @Test
    public void testConnectorWithNoConnectionHasNoList() throws Exception {
        JSONObject entry = listEntry();
        Assert.assertFalse(entry.has("connections"));
        Assert.assertEquals(CONNECTOR, entry.getString("name"));
        Assert.assertFalse(singleEntry().has("connections"));
    }

    @Test
    public void testHealthThatCanNotBePrintedGivesTheNameOnly() throws Exception {
        Map<String, Object> notANumber = new HashMap<>();
        notANumber.put("status", "active");
        notANumber.put("rate", Double.NaN);
        Map<String, Object> inner = new HashMap<>();
        inner.put("rate", Double.POSITIVE_INFINITY);
        Map<String, Object> nested = new HashMap<>();
        nested.put("status", "active");
        nested.put("pool", inner);
        register("alpha", health("active", 5L));
        register("beta", () -> notANumber);
        register("delta", () -> nested);
        register("gamma", ConnectorResourceHealthTest::failingHealth);
        register("zeta", health("breaker_open", 2L));

        for (JSONObject entry : new JSONObject[]{listEntry(), singleEntry()}) {
            JSONArray connections = entry.getJSONArray("connections");
            Assert.assertEquals("Every connection stays in the list", 5, connections.length());
            Assert.assertEquals("alpha", connections.getJSONObject(0).getString("name"));
            Assert.assertEquals("active", connections.getJSONObject(0).getString("status"));
            Assert.assertEquals(5L, connections.getJSONObject(0).getJSONObject("health").getLong("filesDone"));
            assertNameOnly(connections.getJSONObject(1), "beta");
            assertNameOnly(connections.getJSONObject(2), "delta");
            assertNameOnly(connections.getJSONObject(3), "gamma");
            Assert.assertEquals("zeta", connections.getJSONObject(4).getString("name"));
            Assert.assertEquals("breaker_open", connections.getJSONObject(4).getString("status"));
            Assert.assertEquals(2L, connections.getJSONObject(4).getJSONObject("health").getLong("filesDone"));
            Assert.assertNotNull("The entry can be printed", entry.toString(0));
        }
    }

    @Test
    public void testUnreadableMapInsideAListGivesTheNameOnly() throws Exception {
        register("broken", () -> Map.of("status", "active", "pools", List.of(failingHealth())));
        register("healthy", health("active", 5L));
        for (JSONObject entry : new JSONObject[]{listEntry(), singleEntry()}) {
            JSONArray connections = entry.getJSONArray("connections");
            assertNameOnly(connections.getJSONObject(0), "broken");
            Assert.assertEquals(5L, connections.getJSONObject(1).getJSONObject("health").getLong("filesDone"));
        }
    }

    @Test
    public void testStatusAndHealthComeFromOneCopy() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        register("c1", () -> {
            calls.incrementAndGet();
            return changingHealth();
        });
        JSONObject list = listEntry();
        Assert.assertEquals(1, calls.getAndSet(0));
        JSONObject detail = singleEntry();
        Assert.assertEquals(1, calls.get());
        for (JSONObject entry : new JSONObject[]{list, detail}) {
            JSONObject connection = entry.getJSONArray("connections").getJSONObject(0);
            Assert.assertEquals("The status and the health show the same state",
                    connection.getJSONObject("health").getString("status"), connection.getString("status"));
        }
    }

    @Test
    public void testFailedConnectionWithFractionalHealth() throws Exception {
        BigDecimal exact = new BigDecimal("0.12345678901234567890123456789");
        register("c1", () -> Map.of("status", "failed", "ratio", Fraction.ONE_HALF, "exact", exact));

        for (JSONObject entry : new JSONObject[]{listEntry(), singleEntry()}) {
            JSONObject connection = new JSONObject(entry.toString(0)).getJSONArray("connections").getJSONObject(0);
            Assert.assertEquals("failed", connection.getString("status"));
            JSONObject health = connection.getJSONObject("health");
            Assert.assertEquals("failed", health.getString("status"));
            Assert.assertTrue(health.get("ratio") instanceof Number);
            Assert.assertEquals(0.5, health.getDouble("ratio"), 0.0);
            Assert.assertEquals(exact, health.get("exact"));
        }
    }

    @Test
    public void testSameConnectionNameIsIsolatedByLibrary() throws Exception {
        SynapseLibrary standard = library(STANDARD_PACKAGE);
        SynapseLibrary custom = library("pkg.a");
        SynapseLibrary other = library("pkg.b");
        SynapseLibrary unpackaged = library(null);
        SynapseLibrary versioned = new SynapseLibrary("artifact", CONNECTOR, STANDARD_PACKAGE);
        String versionedName = versioned.getQName().toString();
        Supplier<Map<String, Object>> standardHealth = health("active", 4L);
        Supplier<Map<String, Object>> customHealth = health("failed", 1L);
        Supplier<Map<String, Object>> otherHealth = health("recovering", 2L);
        Supplier<Map<String, Object>> unpackagedHealth = health("idle", 3L);
        Supplier<Map<String, Object>> versionedHealth = health("breaker_open", 6L);
        try {
            ConnectionHealthRegistry.register(CONNECTOR, "primary", standardHealth);
            ConnectionHealthRegistry.register("{pkg.a}file", "primary", customHealth);
            ConnectionHealthRegistry.register("{pkg.b}file", "primary", otherHealth);
            ConnectionHealthRegistry.register("{}file", "primary", unpackagedHealth);
            ConnectionHealthRegistry.register(versionedName, "primary", versionedHealth);

            assertPrimaryStatus(standard, "active");
            assertPrimaryStatus(custom, "failed");
            assertPrimaryStatus(other, "recovering");
            assertPrimaryStatus(unpackaged, "idle");
            assertPrimaryStatus(versioned, "breaker_open");
            ConnectionHealthRegistry.unregister("{pkg.a}file", "primary", customHealth);
            Assert.assertFalse(listEntry(custom).has("connections"));
            Assert.assertFalse(singleEntry(custom).has("connections"));
            assertPrimaryStatus(other, "recovering");
            assertPrimaryStatus(versioned, "breaker_open");
            assertPrimaryStatus(standard, "active");
        } finally {
            ConnectionHealthRegistry.unregister(CONNECTOR, "primary", standardHealth);
            ConnectionHealthRegistry.unregister("{pkg.a}file", "primary", customHealth);
            ConnectionHealthRegistry.unregister("{pkg.b}file", "primary", otherHealth);
            ConnectionHealthRegistry.unregister("{}file", "primary", unpackagedHealth);
            ConnectionHealthRegistry.unregister(versionedName, "primary", versionedHealth);
        }
    }

    private static void assertPrimaryStatus(SynapseLibrary library, String status) throws Exception {
        for (JSONObject entry : new JSONObject[]{listEntry(library), singleEntry(library)}) {
            JSONArray connections = entry.getJSONArray("connections");
            Assert.assertEquals(1, connections.length());
            JSONObject primary = connections.getJSONObject(0);
            Assert.assertEquals("primary", primary.getString("name"));
            Assert.assertEquals(status, primary.getString("status"));
            Assert.assertEquals(status, primary.getJSONObject("health").getString("status"));
        }
    }

    @Test
    public void testAtomicCountersKeepTheirNumericValue() throws Exception {
        long count = 9007199254740993L;
        AtomicLong files = new AtomicLong(count);
        AtomicInteger pending = new AtomicInteger(3);
        register("c1", () -> Map.of("files", files, "pending", List.of(pending)));
        JSONObject[] entries = {listEntry(), singleEntry()};
        files.incrementAndGet();
        pending.set(0);
        for (JSONObject entry : entries) {
            JSONObject health = entry.getJSONArray("connections").getJSONObject(0).getJSONObject("health");
            Assert.assertTrue(health.get("files") instanceof Number);
            Assert.assertEquals(0, BigDecimal.valueOf(count).compareTo(health.getBigDecimal("files")));
            Assert.assertTrue(health.getJSONArray("pending").get(0) instanceof Number);
            Assert.assertEquals(0, BigDecimal.valueOf(3).compareTo(
                    health.getJSONArray("pending").getBigDecimal(0)));
        }
    }

    @Test
    public void testAChangeAfterTheCopyDoesNotReachTheEntry() throws Exception {
        Map<String, Object> pool = new HashMap<>();
        pool.put("open", 2L);
        Map<String, Object> health = new HashMap<>();
        health.put("status", "active");
        health.put("pool", pool);
        register("c1", () -> health);

        JSONObject[] entries = {listEntry(), singleEntry()};
        health.put("status", "failed");
        pool.put("open", 0L);
        for (JSONObject entry : entries) {
            JSONObject connection = entry.getJSONArray("connections").getJSONObject(0);
            Assert.assertEquals("active", connection.getString("status"));
            Assert.assertEquals("active", connection.getJSONObject("health").getString("status"));
            Assert.assertEquals(2L, connection.getJSONObject("health").getJSONObject("pool").getLong("open"));
        }
    }

    private void register(String connection, Supplier<Map<String, Object>> health) {
        registered.put(connection, health);
        ConnectionHealthRegistry.register(CONNECTOR, connection, health);
    }

    private static Supplier<Map<String, Object>> health(String status, long filesDone) {
        Map<String, Object> health = new HashMap<>();
        health.put("status", status);
        health.put("filesDone", filesDone);
        return () -> health;
    }

    // A health map of a provider that can not be read.
    private static Map<String, Object> failingHealth() {
        return new AbstractMap<String, Object>() {
            @Override
            public Set<Entry<String, Object>> entrySet() {
                throw new ConcurrentModificationException("health changed");
            }
        };
    }

    // A health map of a provider that shows another status at each read.
    private static Map<String, Object> changingHealth() {
        return new AbstractMap<String, Object>() {
            private int reads;

            @Override
            public Set<Entry<String, Object>> entrySet() {
                reads++;
                return Collections.<String, Object>singletonMap("status", "state" + reads).entrySet();
            }
        };
    }

    private static void assertNameOnly(JSONObject connection, String name) {
        Assert.assertEquals(name, connection.getString("name"));
        Assert.assertEquals("Only the name is printed", 1, connection.length());
    }

    private static SynapseLibrary library(String namespace) {
        SynapseLibrary library = new SynapseLibrary(CONNECTOR, namespace);
        library.setLibStatus(true);
        return library;
    }

    // The entry of the list and of the search.
    private static JSONObject listEntry() throws Exception {
        return listEntry(library(STANDARD_PACKAGE));
    }

    private static JSONObject listEntry(SynapseLibrary library) throws Exception {
        Method add = ConnectorResource.class.getDeclaredMethod("addToJsonList", JSONArray.class,
                SynapseLibrary.class);
        add.setAccessible(true);
        JSONArray list = new JSONArray();
        add.invoke(new ConnectorResource(), list, library);
        return list.getJSONObject(0);
    }

    // The entry of the single view.
    private static JSONObject singleEntry() throws Exception {
        return singleEntry(library(STANDARD_PACKAGE));
    }

    private static JSONObject singleEntry(SynapseLibrary library) throws Exception {
        Method get = ConnectorResource.class.getDeclaredMethod("getConnectorAsJson", SynapseLibrary.class);
        get.setAccessible(true);
        return (JSONObject) get.invoke(new ConnectorResource(), library);
    }
}
