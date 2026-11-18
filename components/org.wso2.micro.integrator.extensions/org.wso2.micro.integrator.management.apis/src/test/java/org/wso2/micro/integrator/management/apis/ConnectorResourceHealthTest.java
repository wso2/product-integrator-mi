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
import java.util.HashMap;
import java.util.Map;
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
