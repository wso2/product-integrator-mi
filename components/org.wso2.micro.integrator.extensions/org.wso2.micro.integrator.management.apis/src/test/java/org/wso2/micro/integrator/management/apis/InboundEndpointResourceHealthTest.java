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

import com.google.gson.JsonObject;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axis2.context.ConfigurationContext;
import org.apache.axis2.context.MessageContext;
import org.apache.axis2.engine.AxisConfiguration;
import org.apache.commons.lang3.math.Fraction;
import org.apache.synapse.aspects.AspectConfiguration;
import org.apache.synapse.config.SynapseConfiguration;
import org.apache.synapse.core.axis2.Axis2MessageContext;
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
import java.util.AbstractMap;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;
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
        Assert.assertEquals("The status and the health show the same state", "degraded",
                inbound.getJSONObject("health").getString(Constants.STATUS));
        Assert.assertEquals("The status of the list comes from the same health", "degraded", listStatus());
    }

    @Test
    public void testEventBasedRegistryIsAskedFirst() throws Exception {
        Map<String, Object> eventHealth = new HashMap<>();
        eventHealth.put(Constants.STATUS, "connected");
        Map<String, Object> pollingHealth = new HashMap<>();
        pollingHealth.put(Constants.STATUS, "degraded");
        GenericEventBasedConsumerRegistry.register(NAME, new HealthEventBasedConsumer(eventHealth));
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> pollingHealth));

        Assert.assertEquals("connected", consumerHealth(inboundEndpoint()).getString(Constants.STATUS));
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

    @Test
    public void testFailedPollingConsumerWithFractionalHealth() throws Exception {
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(
                () -> Map.of(Constants.STATUS, "failed", "ratio", Fraction.ONE_HALF)));

        Assert.assertEquals("failed", listStatus());
        JSONObject inbound = new JSONObject(convert(inboundEndpoint()).toString(0));
        Assert.assertEquals("failed", inbound.getString(Constants.STATUS));
        JSONObject health = inbound.getJSONObject("health");
        Assert.assertEquals("failed", health.getString(Constants.STATUS));
        Assert.assertTrue(health.get("ratio") instanceof Number);
        Assert.assertEquals(0.5, health.getDouble("ratio"), 0.0);
    }

    @Test
    public void testNonfiniteNumericConversionStillFallsBack() throws Exception {
        DoubleAdder rate = new DoubleAdder();
        rate.add(Double.NEGATIVE_INFINITY);
        GenericPollingConsumerRegistry.register(NAME,
                new HealthPollingConsumer(() -> Map.of(Constants.STATUS, "failed", "rate", rate)));
        assertFallback();
    }

    @Test
    public void testPollingHealthThatCanNotBePrintedGivesNoHealth() throws Exception {
        Map<String, Object> health = new HashMap<>();
        health.put(Constants.STATUS, "failed");
        health.put("rate", Double.POSITIVE_INFINITY);
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> health));

        assertFallback();
    }

    @Test
    public void testPollingHealthThatCanNotBeReadGivesNoHealth() throws Exception {
        GenericPollingConsumerRegistry.register(NAME,
                new HealthPollingConsumer(InboundEndpointResourceHealthTest::failingHealth));

        assertFallback();
    }

    @Test
    public void testUnreadableNestedHealthFallsBackInListAndDetail() throws Exception {
        GenericPollingConsumerRegistry.register(NAME,
                new HealthPollingConsumer(() -> Map.of(Constants.STATUS, "failed", "pool", failingHealth())));
        assertFallback();
    }

    @Test
    public void testEventBasedHealthThatCanNotBePrintedGivesNoHealth() throws Exception {
        Map<String, Object> eventHealth = new HashMap<>();
        eventHealth.put(Constants.STATUS, "connected");
        eventHealth.put("rate", Double.NaN);
        Map<String, Object> pollingHealth = new HashMap<>();
        pollingHealth.put(Constants.STATUS, "degraded");
        GenericEventBasedConsumerRegistry.register(NAME, new HealthEventBasedConsumer(eventHealth));
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> pollingHealth));

        // The event based consumer is still the one that is asked. The polling one does not take its place.
        assertFallback();
    }

    @Test
    public void testStatusAndHealthOfAPollingConsumerComeFromOneCopy() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> {
            calls.incrementAndGet();
            return changingHealth();
        }));
        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertEquals(1, calls.getAndSet(0));
        listStatus();
        Assert.assertEquals(1, calls.get());
        Assert.assertEquals("The status and the health show the same state",
                inbound.getJSONObject("health").getString(Constants.STATUS), inbound.getString(Constants.STATUS));
    }

    @Test
    public void testAChangeAfterTheCopyDoesNotReachTheResponse() throws Exception {
        Map<String, Object> health = new HashMap<>();
        health.put(Constants.STATUS, "active");
        GenericPollingConsumerRegistry.register(NAME, new HealthPollingConsumer(() -> health));

        JSONObject inbound = convert(inboundEndpoint());
        health.put(Constants.STATUS, "failed");
        Assert.assertEquals("active", inbound.getString(Constants.STATUS));
        Assert.assertEquals("active", inbound.getJSONObject("health").getString(Constants.STATUS));
    }

    // With no health the single view and the list show the status of the endpoint and no health.
    private static void assertFallback() throws Exception {
        JSONObject inbound = convert(inboundEndpoint());
        Assert.assertNull(consumerHealth(inboundEndpoint()));
        Assert.assertEquals(fallbackStatus(), inbound.getString(Constants.STATUS));
        Assert.assertFalse(inbound.has("health"));
        Assert.assertEquals(fallbackStatus(), listStatus());
        Assert.assertNotNull("The response can be printed", inbound.toString(0));
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

    private static JSONObject consumerHealth(InboundEndpoint inboundEndpoint) throws Exception {
        Method lookup = InboundEndpointResource.class.getDeclaredMethod("getConsumerHealth", InboundEndpoint.class);
        lookup.setAccessible(true);
        return (JSONObject) lookup.invoke(new InboundEndpointResource("/inbound-endpoints"), inboundEndpoint);
    }

    private static String listStatus() throws Exception {
        SynapseConfiguration config = new SynapseConfiguration();
        config.addInboundEndpoint(NAME, inboundEndpoint());
        MessageContext axis = new MessageContext();
        axis.setConfigurationContext(new ConfigurationContext(new AxisConfiguration()));
        axis.setEnvelope(OMAbstractFactory.getSOAP11Factory().getDefaultEnvelope());
        Axis2MessageContext context = new Axis2MessageContext(axis, config, null);
        Method list = InboundEndpointResource.class.getDeclaredMethod("populateInboundEndpointList",
                org.apache.synapse.MessageContext.class);
        list.setAccessible(true);
        list.invoke(new InboundEndpointResource("/inbound-endpoints"), context);
        JsonObject entry = Utils.getJsonPayload(axis).getAsJsonArray(Constants.LIST).get(0).getAsJsonObject();
        Assert.assertEquals(NAME, entry.get(Constants.NAME).getAsString());
        return entry.get(Constants.STATUS).getAsString();
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
