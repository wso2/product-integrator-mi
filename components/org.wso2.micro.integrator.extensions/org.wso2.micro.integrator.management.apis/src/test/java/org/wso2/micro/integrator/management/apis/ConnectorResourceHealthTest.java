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

import org.apache.synapse.libraries.model.SynapseLibrary;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.connector.core.ConnectionHealthRegistry;

import java.lang.reflect.Method;
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
            Assert.assertEquals(count, health.getLong("files"));
            Assert.assertTrue(health.getJSONArray("pending").get(0) instanceof Number);
            Assert.assertEquals(3, health.getJSONArray("pending").getInt(0));
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

    private static SynapseLibrary library() {
        SynapseLibrary library = new SynapseLibrary(CONNECTOR, "org.wso2.carbon.connector");
        library.setLibStatus(true);
        return library;
    }

    // The entry of the list and of the search.
    private static JSONObject listEntry() throws Exception {
        Method add = ConnectorResource.class.getDeclaredMethod("addToJsonList", JSONArray.class,
                SynapseLibrary.class);
        add.setAccessible(true);
        JSONArray list = new JSONArray();
        add.invoke(new ConnectorResource(), list, library());
        return list.getJSONObject(0);
    }

    // The entry of the single view.
    private static JSONObject singleEntry() throws Exception {
        Method get = ConnectorResource.class.getDeclaredMethod("getConnectorAsJson", SynapseLibrary.class);
        get.setAccessible(true);
        return (JSONObject) get.invoke(new ConnectorResource(), library());
    }
}
