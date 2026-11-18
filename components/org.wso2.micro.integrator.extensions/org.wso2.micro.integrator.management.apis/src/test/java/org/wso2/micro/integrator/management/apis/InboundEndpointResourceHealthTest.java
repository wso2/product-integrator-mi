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

import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.inbound.InboundEndpoint;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumer;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericEventBasedConsumerRegistry;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumerRegistry;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

public class InboundEndpointResourceHealthTest {

    private static final String NAME = "FileHealthInbound";

    @After
    public void clear() {
        GenericPollingConsumerRegistry.unregister(NAME, GenericPollingConsumerRegistry.get(NAME));
        GenericEventBasedConsumerRegistry.unregister(NAME, GenericEventBasedConsumerRegistry.get(NAME));
    }

    @Test
    public void testHealthOfAPollingConsumer() throws Exception {
        Map<String, Object> health = new HashMap<>();
        health.put(Constants.STATUS, "degraded");
        health.put("filesDone", 3L);
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> health));

        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertEquals("The status comes from the health of the polling consumer", "degraded",
                inbound.getString(Constants.STATUS));
        Assert.assertEquals("The whole health map is printed", 3L,
                inbound.getJSONObject("health").getLong("filesDone"));
        Assert.assertSame(health, consumerHealth(inboundEndpoint()));
    }

    @Test
    public void testEventBasedRegistryIsAskedFirst() throws Exception {
        Map<String, Object> eventHealth = new HashMap<>();
        eventHealth.put(Constants.STATUS, "connected");
        Map<String, Object> pollingHealth = new HashMap<>();
        pollingHealth.put(Constants.STATUS, "degraded");
        GenericEventBasedConsumerRegistry.register(NAME, new HealthEventBasedConsumer(eventHealth));
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> pollingHealth));

        Assert.assertSame(eventHealth, consumerHealth(inboundEndpoint()));
        Assert.assertEquals("connected", convert(inboundEndpoint()).getString(Constants.STATUS));
    }

    @Test
    public void testNoConsumerGivesNoHealth() throws Exception {
        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertNull(consumerHealth(inboundEndpoint()));
        Assert.assertEquals("With no health the status is the one of the endpoint", fallbackStatus(),
                inbound.getString(Constants.STATUS));
        Assert.assertFalse(inbound.has("health"));
    }

    @Test
    public void testAPollingConsumerWithNoHealthGivesNoHealth() throws Exception {
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(null));

        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertEquals("With no health the status is the one of the endpoint", fallbackStatus(),
                inbound.getString(Constants.STATUS));
        Assert.assertFalse(inbound.has("health"));
    }

    @Test
    public void testAFailingHealthCallGivesNoHealth() throws Exception {
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> {
            throw new IllegalStateException("health failed");
        }));

        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertNull(consumerHealth(inboundEndpoint()));
        Assert.assertEquals("With no health the status is the one of the endpoint", fallbackStatus(),
                inbound.getString(Constants.STATUS));
        Assert.assertFalse(inbound.has("health"));
    }

    private static InboundEndpoint inboundEndpoint() {
        InboundEndpoint inboundEndpoint = new InboundEndpoint();
        inboundEndpoint.setName(NAME);
        inboundEndpoint.setProtocol("file");
        inboundEndpoint.setInjectingSeq("main");
        inboundEndpoint.configure(new AspectConfiguration(NAME));
        return inboundEndpoint;
    }

    private static String fallbackStatus() {
        return inboundEndpoint().isDeactivated() ? Constants.INACTIVE_STATUS : Constants.ACTIVE_STATUS;
    }

    private static JSONObject convert(InboundEndpoint inboundEndpoint) throws Exception {
        Method conversion = InboundEndpointResource.class
                .getDeclaredMethod("convertInboundEndpointToJsonObject", InboundEndpoint.class);
        conversion.setAccessible(true);
        return (JSONObject) conversion.invoke(new InboundEndpointResource("/inbound-endpoints"), inboundEndpoint);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> consumerHealth(InboundEndpoint inboundEndpoint) throws Exception {
        Method lookup = InboundEndpointResource.class.getDeclaredMethod("getConsumerHealth", InboundEndpoint.class);
        lookup.setAccessible(true);
        return (Map<String, Object>) lookup.invoke(new InboundEndpointResource("/inbound-endpoints"),
                inboundEndpoint);
    }

    private static class HealthPollingConsumer extends GenericPollingConsumer {

        private final Supplier<Map<String, Object>> health;

        HealthPollingConsumer(Supplier<Map<String, Object>> health) {
            super(new Properties(), NAME, null, 1000L, "main", "fault", false, true);
            this.health = health;
        }

        @Override
        public Map<String, Object> getHealth() {
            return health == null ? super.getHealth() : health.get();
        }

        @Override
        public Object poll() {
            return null;
        }

        @Override
        public void resume() {
        }

        @Override
        public void pause() {
        }
    }

    private static class HealthEventBasedConsumer extends GenericEventBasedConsumer {

        private final Map<String, Object> health;

        HealthEventBasedConsumer(Map<String, Object> health) {
            super(new Properties(), NAME, null, "main", "fault", false, true);
            this.health = health;
        }

        @Override
        public Map<String, Object> getHealth() {
            return health;
        }

        @Override
        public void listen() {
        }

        @Override
        public void destroy() {
        }

        @Override
        public void resume() {
        }

        @Override
        public void pause() {
        }
    }
}
